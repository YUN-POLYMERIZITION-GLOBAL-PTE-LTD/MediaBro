#!/usr/bin/env python3
"""
OpenClaw Skill: Media File Server with FRP + WebRTC P2P Support

Architecture:
  - Flask serves HTTP routes (API and player page)
  - Flask-SocketIO handles WebSocket signaling (same port as Flask)
  - aiortc handles WebRTC P2P data channels for media streaming
  - An asyncio event loop runs in a background thread for aiortc operations
  - FRP tunnels signaling and the player page through FRPS
  - WebRTC P2P bypasses FRPS for actual media data, saving bandwidth
"""

import os
import json
import glob
import hashlib
import asyncio
import threading
import logging
import math
from urllib.parse import quote
from pathlib import Path

from flask import Flask, render_template, abort, jsonify
from flask import request
from flask_socketio import SocketIO
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCDataChannel, RTCIceCandidate, RTCConfiguration, RTCIceServer
from aiortc.exceptions import InvalidStateError
from aiortc.sdp import candidate_from_sdp
import concurrent.futures
import aiofiles

from media_frp_util import get_domain, setup_frp
from media_file_util import get_current_directory, get_media_directory, get_media_files
from media_server_log import info, debug
from media_streaming import stream_metadata, fragmented_mp4
from media_ice_compat import install_stun_retry_guard, install_windows_udp_close_guard

install_stun_retry_guard()
install_windows_udp_close_guard()

# ── Initialize Flask + SocketIO (same port, no extra FRP proxy needed) ──
app = Flask(__name__, template_folder='templates')


def signaling_origin_allowed(origin, environ):
    """Accept the sharing page / native WebView and preserve same-origin access."""
    if not origin:
        return True  # Native Socket.IO clients may omit Origin entirely.
    configured = os.environ.get(
        "MEDIA_SIGNALING_ORIGINS", "https://yun-hub.chat,http://yun-hub.chat,null"
    )
    allowed = {value.strip() for value in configured.split(',') if value.strip()}
    host = environ.get('HTTP_HOST')
    scheme = environ.get('wsgi.url_scheme')
    if host and scheme:
        allowed.add(f"{scheme}://{host}")
        forwarded_host = environ.get('HTTP_X_FORWARDED_HOST', host).split(',')[0].strip()
        forwarded_scheme = environ.get('HTTP_X_FORWARDED_PROTO', scheme).split(',')[0].strip()
        allowed.add(f"{forwarded_scheme}://{forwarded_host}")
    return origin in allowed or '*' in allowed


socketio = SocketIO(
    app, async_mode="threading", cors_allowed_origins=signaling_origin_allowed
)

# ── Global state for WebRTC ──
async_loop = None          # asyncio event loop running in a background thread
loop_ready = threading.Event()  # signals when async_loop is initialized
peer_connections = {}      # sid -> RTCPeerConnection
pc_lock = threading.Lock()  # protects peer_connections dict
DISCONNECT_IDLE_TIMEOUT = 30.0
TRANSFER_STALL_TIMEOUT = 60.0
CLOSE_TIMEOUT = 5.0
NEGOTIATION_TIMEOUT = 30.0
MAX_PENDING_CANDIDATES = 128
background_file_tasks = set()


def build_ice_configuration():
    """aiortc supports one STUN and one TURN; do not imply STUN failover."""
    stun_url = os.environ.get('MEDIA_STUN_URL', 'stun:stun.cloudflare.com:3478').strip()
    servers = [RTCIceServer(urls=stun_url)] if stun_url else []
    turn_url = os.environ.get('MEDIA_TURN_URL', '').strip()
    if turn_url:
        username = os.environ.get('MEDIA_TURN_USERNAME', '')
        credential = os.environ.get('MEDIA_TURN_PASSWORD', '')
        if not username or not credential:
            raise ValueError('MEDIA_TURN_URL requires MEDIA_TURN_USERNAME and MEDIA_TURN_PASSWORD')
        servers.append(RTCIceServer(urls=turn_url, username=username, credential=credential))
    return RTCConfiguration(iceServers=servers)


def candidate_type_counts(sdp):
    counts = {'host': 0, 'srflx': 0, 'relay': 0, 'prflx': 0}
    for line in sdp.splitlines():
        if line.startswith('a=candidate:'):
            fields = line.split()
            try:
                candidate_type = fields[fields.index('typ') + 1]
            except (ValueError, IndexError):
                continue
            counts[candidate_type] = counts.get(candidate_type, 0) + 1
    return counts


def report_task_error(task):
    """Consume background failures even when optional file logging is disabled."""
    try:
        task.result()
    except (asyncio.CancelledError, concurrent.futures.CancelledError):
        pass
    except Exception:
        info("WebRTC background task failed")


def track_file_task(pc, awaitable):
    task = asyncio.ensure_future(awaitable)
    pc.file_tasks.add(task)
    background_file_tasks.add(task)

    def finished(done):
        pc.file_tasks.discard(done)
        background_file_tasks.discard(done)
        report_task_error(done)

    task.add_done_callback(finished)
    return task


async def open_media_file(pc, filepath):
    task = track_file_task(pc, aiofiles.open(filepath, "rb"))
    try:
        return await asyncio.wait_for(asyncio.shield(task), TRANSFER_STALL_TIMEOUT)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        async def release_late_file():
            f = await asyncio.shield(task)
            await f.close()
        track_file_task(pc, release_late_file())
        raise


async def close_peer(sid, pc):
    """Bound cleanup waits and retain ownership of late transport closes."""
    if pc.cleanup_task is None:
        pc.closing = True

        async def cleanup():
            try:
                tasks = set(pc.media_tasks)
                if pc.offer_task is not None and not pc.offer_task.done():
                    tasks.add(pc.offer_task)
                for task in tasks:
                    task.cancel()
                if tasks:
                    _, pending = await asyncio.wait(tasks, timeout=CLOSE_TIMEOUT)
                    if pending:
                        info(f"Peer {sid} has {len(pending)} pending tasks")

                if pc.transport_close_task is None:
                    pc.transport_close_task = asyncio.create_task(pc.close())

                    def finish_transport_close(task):
                        try:
                            task.result()
                        except asyncio.CancelledError:
                            return
                        except Exception:
                            info(f"Peer close failed: {sid}", )
                        else:
                            with pc_lock:
                                if peer_connections.get(sid) is pc:
                                    peer_connections.pop(sid, None)

                    pc.transport_close_task.add_done_callback(finish_transport_close)
                # aiortc closes only once. Re-invoking a failed close cannot repair
                # its internal completion Future; timeout leaves this task alive.
                done, _ = await asyncio.wait({pc.transport_close_task}, timeout=CLOSE_TIMEOUT)
                if not done:
                    # Keep this cleanup attempt and its transport task owned.
                    # Repeated state/disconnect events must not restart the
                    # same wait or report the same timeout through each caller.
                    info(
                        f"Peer {sid}: transport close still pending after {CLOSE_TIMEOUT:.1f}s; "
                        f"connection={pc.connectionState}, ICE={pc.iceConnectionState}, waiting={describe_task_wait(pc.transport_close_task)}")
                else:
                    pc.transport_close_task.result()
            except BaseException:
                pc.cleanup_task = None
                raise

        pc.cleanup_task = asyncio.create_task(cleanup())
        pc.cleanup_task.add_done_callback(report_task_error)
    await asyncio.shield(pc.cleanup_task)


def describe_task_wait(task):
    """Include nested coroutine locations when a transport close stalls."""
    locations = []
    current = task.get_coro()
    while current is not None:
        frame = getattr(current, 'cr_frame', None) or getattr(current, 'gi_frame', None)
        if frame is not None:
            locations.append(f"{frame.f_code.co_name}:{frame.f_lineno}")
        current = getattr(current, 'cr_await', None) or getattr(current, 'gi_yieldfrom', None)
    return ' -> '.join(locations) or repr(task)


async def watch_disconnected_peer(sid, pc):
    """Allow active P2P transfers to finish, then expire abandoned sessions."""
    loop = asyncio.get_running_loop()
    idle_since = loop.time()
    buffer_progress = {}
    while True:
        if pc.closing or pc.connectionState in ("failed", "closed"):
            break
        channels = [c for c in pc.media_channels if c.readyState == "open"]
        transferring = any(not task.done() for task in pc.media_tasks)
        buffered = False
        now = loop.time()
        for channel in channels:
            amount = channel.bufferedAmount
            previous, changed_at = buffer_progress.get(channel, (amount, now))
            if amount != previous:
                changed_at = now
            changed_at = max(changed_at, getattr(channel, "media_last_send", changed_at))
            buffer_progress[channel] = (amount, changed_at)
            if amount > 0:
                buffered = True
                if now - changed_at >= TRANSFER_STALL_TIMEOUT:
                    await close_peer(sid, pc)
                    return
        buffer_progress = {c: value for c, value in buffer_progress.items() if c in channels}
        if transferring or buffered:
            idle_since = now
        elif pc.media_channel_seen and not channels:
            break
        elif now - idle_since >= DISCONNECT_IDLE_TIMEOUT:
            break
        await asyncio.sleep(0.25)
    await close_peer(sid, pc)


async def retire_peer(sid, pc):
    pc.signaling_disconnected = True
    if pc.disconnect_task is None or pc.disconnect_task.done():
        pc.disconnect_task = asyncio.create_task(watch_disconnected_peer(sid, pc))
        pc.disconnect_task.add_done_callback(report_task_error)

def generate_md5_checksum(s: str) -> str:
    encoded_data = s.encode('utf-8')
    return hashlib.md5(encoded_data).hexdigest()

# ══════════════════════════════════════════════════════════════
#  Flask HTTP Routes
# ══════════════════════════════════════════════════════════════

@app.route('/api/find_file', methods=['POST'])
def handle_api_find_file():
    """WhatsApp API endpoint — returns a playlist of WebRTC player URLs."""
    try:
        data = request.json
        if not isinstance(data, dict):
            return jsonify({"error": "Invalid JSON format"}), 400

        required = ('user_id', 'player_id', 'media_name', 'pay_time', 'pay_count')
        if any(data.get(key) is None for key in required):
            return jsonify({"error": "Missing required fields"}), 400

        user_id = data.get('user_id')
        player_id = data.get('player_id')
        media_name = data.get('media_name')
        pay_time = data.get('pay_time')
        pay_count = data.get('pay_count')

        base_dir = get_media_directory()
        search_pattern = os.path.join(base_dir, f"*{media_name}*")
        matched_files = glob.glob(search_pattern)
        media_files = [os.path.basename(f) for f in matched_files if os.path.isfile(f)]

        frp_domain = get_domain()
        txt = " *The file below is a playlist of all available media files:*\n\n"
        if isinstance(media_files, list) and len(media_files) > 0:
            for media_file in media_files:
                media_file_name = os.path.basename(media_file)
                media_file_url = f"http://{frp_domain}/{media_file_name}"
                query_dp = f"videourl={media_file_url}&userid={user_id}&playerid={player_id}&paytime={pay_time}&paycount={pay_count}"
                query_cs = generate_md5_checksum(query_dp)
                query_dp += f"&cks={query_cs}"

                player_url = f"https://yun-hub.chat/link/?app=aipollo&clickid=12345&dplink={quote(query_dp, safe='')}"
                txt += f"{media_file_name}: {player_url}\n"
        else:
            txt += "No media files found."
        return jsonify({"text": txt}), 200
    except Exception as e:
        info(f"ERROR in handle_api_list_files: {e}")
        return jsonify({"error": str(e)}), 500

def resolve_media_file(filename):
    """Resolve a top-level MP4 without allowing traversal or external symlinks."""
    if not filename or filename != os.path.basename(filename) or "\\" in filename:
        abort(404)

    if Path(filename).suffix.lower() != ".mp4":
        abort(404)

    try:
        root = Path(get_media_directory()).resolve()
        path = (root / filename).resolve(strict=True)
        path.relative_to(root)
        if not path.is_file():
            abort(404)
    except (OSError, ValueError, RuntimeError):
        abort(404)
    return path


@app.route('/play')
def serve_player():
    filename = request.args.get('file')
    if not filename:
        abort(400)
    resolve_media_file(filename)
    return render_template('player.html', filename=filename)


# Media files are delivered only over the WebRTC DataChannel.

# ══════════════════════════════════════════════════════════════
#  WebRTC Signaling (Flask-SocketIO WebSocket handlers)
# ══════════════════════════════════════════════════════════════

@socketio.on('connect')
def handle_connect():
    """Create a new RTCPeerConnection for each connected client."""
    sid = request.sid
    try:
        configuration = build_ice_configuration()
    except ValueError:
        info('Invalid ICE server configuration')
        return False
    pc = RTCPeerConnection(configuration)
    pc.media_channels = set()
    pc.media_tasks = set()
    pc.file_tasks = set()
    pc.offer_task = None
    pc.remote_description_ready = False
    pc.pending_candidates = []
    pc.media_channel_seen = False
    pc.disconnect_task = None
    pc.cleanup_task = None
    pc.transport_close_task = None
    pc.closing = False
    pc.signaling_disconnected = False
    with pc_lock:
        peer_connections[sid] = pc
    info(f"[WebRTC] Client connected: {sid} (active sessions: {len(peer_connections)})")
    info(f"Signaling connected: {sid}, origin={request.headers.get('Origin')}")
    # aiortc gathers local candidates in setLocalDescription(); the complete
    # answer SDP carries them. It does not emit browser-style icecandidate events.

    @pc.on("datachannel")
    def on_datachannel(channel):
        info(f"[WebRTC] DataChannel opened for {sid}")
        info(f"Peer {sid}: DataChannel received, state={channel.readyState}")
        pc.media_channels.add(channel)
        pc.media_channel_seen = True

        stream_task = None
        receiver_ready = asyncio.Event()
        receiver_ready.set()
        last_playback_report = 0

        @channel.on("message")
        def on_message(message):
            nonlocal stream_task, last_playback_report

            if message == 'pause':
                if receiver_ready.is_set():
                    info(f'Peer {sid}: receiver paused stream')
                receiver_ready.clear()
                return
            if message == 'resume':
                if not receiver_ready.is_set():
                    info(f'Peer {sid}: receiver resumed stream')
                receiver_ready.set()
                return
            if isinstance(message, str) and message.startswith('playback:'):
                now = asyncio.get_running_loop().time()
                if now - last_playback_report < 1 or len(message) > 2048:
                    return
                last_playback_report = now
                try:
                    data = json.loads(message[9:])
                    if not isinstance(data, dict) or data.get('event') not in ('playing', 'waiting', 'seeking', 'progress', 'error'):
                        return
                    values = [data.get(key, 0) for key in ('time', 'ahead', 'bytes', 'queued', 'ready')]
                    if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in values):
                        return
                    info(
                        f"Peer {sid}: playback={data['event']}, time={values[0]:.2f}, ahead={values[1]:.2f}, "
                        f"received={values[2]:d}, queued={values[3]:d}, ready={values[4]:d}, "
                        f"paused={data.get('paused') is True}, receiver_paused={data.get('receiverPaused') is True}, "
                        f"reason={str(data.get('reason', ''))[:200]!r}"
                    )
                except (ValueError, TypeError):
                    pass
                return
            if not isinstance(message, str) or not message.startswith(("request:", "stream:")):
                return
            if channel.readyState != "open" or pc.closing:
                return

            if stream_task is not None and not stream_task.done():
                channel.send(json.dumps({"error": "已有文件正在传输"}))
                return

            filename = os.path.basename(message.split(":", 1)[1])
            receiver_ready.set()
            handler = stream_fragments_async if message.startswith('stream:') else stream_file_async
            stream_task = asyncio.create_task(handler(filename))
            pc.media_tasks.add(stream_task)

            def transfer_finished(task):
                pc.media_tasks.discard(task)
                report_task_error(task)

            stream_task.add_done_callback(transfer_finished)

        @channel.on("close")
        def on_close():
            pc.media_channels.discard(channel)
            if stream_task is not None and not stream_task.done():
                stream_task.cancel()

        HIGH = 1024 * 1024
        LOW = 256 * 1024
        channel.bufferedAmountLowThreshold = LOW
        buffer_changed = asyncio.Event()

        @channel.on("bufferedamountlow")
        def on_buffer_low():
            buffer_changed.set()

        @channel.on("close")
        def on_buffer_closed():
            buffer_changed.set()

        async def wait_for_capacity():
            if channel.readyState != "open":
                raise ConnectionError("DataChannel 已关闭")
            if channel.bufferedAmount <= HIGH:
                return

            loop = asyncio.get_running_loop()
            last_amount = channel.bufferedAmount
            last_progress = loop.time()
            while channel.bufferedAmount > LOW:
                if channel.readyState != "open":
                    raise ConnectionError("DataChannel 已关闭")

                buffer_changed.clear()
                remaining = TRANSFER_STALL_TIMEOUT - (loop.time() - last_progress)
                if remaining <= 0:
                    raise TimeoutError("DataChannel 发送缓冲区长时间没有进展")
                try:
                    await asyncio.wait_for(buffer_changed.wait(), min(1.0, remaining))
                except asyncio.TimeoutError:
                    pass
                amount = channel.bufferedAmount
                if amount < last_amount:
                    last_progress = loop.time()
                last_amount = amount

            if channel.readyState != "open":
                raise ConnectionError("DataChannel 已关闭")

        async def stream_fragments_async(filename):
            fragments = None
            size = 0
            info(f"Peer {sid}: streaming request={filename}")
            try:
                filepath = resolve_media_file(filename)
                metadata = await asyncio.wait_for(asyncio.to_thread(stream_metadata, filepath), 15)
                info(f"Peer {sid}: stream MIME={metadata['mime']}, duration={metadata['duration']:.2f}")
                channel.send(json.dumps(metadata))
                fragments = fragmented_mp4(filepath)
                info(f"Peer {sid}: fragmented stream started {fragments}")
                last_progress = 0
                async for chunk in fragments:
                    await receiver_ready.wait()
                    await wait_for_capacity()
                    channel.send(chunk)
                    size += len(chunk)
                    now = asyncio.get_running_loop().time()
                    channel.media_last_send = now
                    if now - last_progress >= 10:
                        info(f"Peer {sid}: streamed={size:d}, send_buffer={channel.bufferedAmount:d}")
                        last_progress = now
                channel.send(json.dumps({'type': 'done', 'size': size}))
                info(f"Peer {sid}: stream completed, sent={size:d}")
            except asyncio.CancelledError:
                info(f"Peer {sid}: stream cancelled, sent={size:d}")
                raise
            except Exception as exc:
                info(f"P2P streaming failed for {sid}")
                if channel.readyState == 'open':
                    channel.send(json.dumps({'error': str(exc)}))
                    channel.close()
            finally:
                if fragments is not None:
                    await fragments.aclose()

        async def stream_file_async(filename):
            filepath = os.path.join(get_media_directory(), filename)
            info(f"Peer {sid}: media request={filename}")
            try:
                f = await open_media_file(pc, filepath)
                file_operations = set()

                async def file_operation(awaitable):
                    task = track_file_task(pc, awaitable)
                    file_operations.add(task)
                    task.add_done_callback(file_operations.discard)
                    return await asyncio.wait_for(asyncio.shield(task), TRANSFER_STALL_TIMEOUT)

                try:
                    stat = await file_operation(asyncio.to_thread(os.fstat, f.fileno()))
                    channel.send(json.dumps({
                        "type": "meta", "size": stat.st_size, "name": filename,
                    }))
                    while True:
                        await wait_for_capacity()
                        chunk = await file_operation(f.read(64 * 1024))
                        if not chunk:
                            break
                        channel.send(chunk)
                        channel.media_last_send = asyncio.get_running_loop().time()
                    if channel.readyState == "open":
                        channel.send(json.dumps({"type": "done"}))
                        info(f"Peer {sid}: stream completed, sent={stat.st_size:d}")
                finally:
                    pending_operations = tuple(file_operations)

                    async def release_file():
                        # Do not close a descriptor while a worker still uses it.
                        if pending_operations:
                            await asyncio.gather(*pending_operations, return_exceptions=True)
                        await f.close()

                    close_task = track_file_task(pc, release_file())
                    await asyncio.wait_for(asyncio.shield(close_task), CLOSE_TIMEOUT)
            except asyncio.CancelledError:
                raise
            except Exception:
                info(f"Media transfer failed for {sid}")
                if channel.readyState == "open":
                    channel.send(json.dumps({"error": "文件传输失败或超时"}))
                channel.close()

    @pc.on("connectionstatechange")
    def on_connection_state():
        state = pc.connectionState
        info(f"[WebRTC] Connection state for {sid}: {state}")
        info(f"Peer {sid}: connection={state}, ICE={pc.iceConnectionState}")
        if state in ("closed", "failed"):
            if not pc.closing and not pc.signaling_disconnected and state == "closed":
                socketio.emit('signaling_error', {
                    'error': 'P2P 连接已关闭，网络保活可能已失效',
                    'code': 'peer_closed',
                }, room=sid)
            task = asyncio.create_task(close_peer(sid, pc))
            task.add_done_callback(report_task_error)

    @pc.on("iceconnectionstatechange")
    def on_ice_state():
        info(f"Peer {sid}: ICE state={pc.iceConnectionState}")
        if pc.iceConnectionState == 'failed':
            info(
                f"Peer {sid}: ICE checks failed; inspect UDP reachability and NAT/TURN configuration"
            )
            if not pc.signaling_disconnected:
                socketio.emit('signaling_error', {
                    'error': 'ICE 连通性检查失败；请检查 UDP 网络或配置 TURN 中继',
                    'code': 'ice_failed',
                }, room=sid)
            task = asyncio.create_task(close_peer(sid, pc))
            task.add_done_callback(report_task_error)

async def process_offer(sid, pc, sdp):
    if pc.closing:
        return
    if pc.offer_task is not None and not pc.offer_task.done():
        socketio.emit("signaling_error", {"error": "已有协商正在进行"}, room=sid)
        return

    async def negotiate():
        try:
            async with asyncio.timeout(NEGOTIATION_TIMEOUT):
                info(f"Peer {sid}: applying remote offer")
                media = [line.split()[0][2:] for line in sdp.splitlines() if line.startswith('m=')]
                info(f"Peer {sid}: remote media={media}, SDP candidates={candidate_type_counts(sdp)}")
                await pc.setRemoteDescription(RTCSessionDescription(sdp=sdp, type="offer"))
                if pc.closing:
                    return
                pc.remote_description_ready = True
                candidates, pc.pending_candidates = pc.pending_candidates, []
                for candidate in candidates:
                    await pc.addIceCandidate(candidate)
                answer = await pc.createAnswer()
                if pc.closing:
                    return
                await pc.setLocalDescription(answer)
                counts = candidate_type_counts(pc.localDescription.sdp)
                info(f"Peer {sid}: local answer candidates={counts}")
                if not counts['srflx'] and not counts['relay']:
                    info(f"Peer {sid}: no public or relay candidate gathered")
                if not pc.closing and not pc.signaling_disconnected:
                    socketio.emit('answer', {
                        'type': 'answer', 'sdp': pc.localDescription.sdp,
                    }, room=sid)
                    info(f"Peer {sid}: answer sent after ICE gathering")
        except asyncio.CancelledError:
            raise
        except Exception:
            info(f"Offer processing failed for {sid}")
            socketio.emit("signaling_error", {"error": "协商失败或超时"}, room=sid)
            # Schedule cleanup after this task returns; do not wait on our own cancellation.
            task = asyncio.create_task(close_peer(sid, pc))
            task.add_done_callback(report_task_error)

    pc.offer_task = asyncio.create_task(negotiate())
    pc.offer_task.add_done_callback(report_task_error)


@socketio.on('offer')
def handle_offer(data):
    sid = request.sid
    if not isinstance(data, dict) or not isinstance(data.get('sdp'), str) or not data['sdp']:
        socketio.emit("signaling_error", {"error": "无效 SDP"}, room=sid)
        return
    with pc_lock:
        pc = peer_connections.get(sid)
    if pc is not None:
        future = asyncio.run_coroutine_threadsafe(process_offer(sid, pc, data['sdp']), async_loop)
        future.add_done_callback(report_task_error)


async def process_candidate(sid, pc, data):
    if pc.closing:
        return
    try:
        candidate_str = data.get('candidate')
        if candidate_str is None or candidate_str == "":
            candidate = None
        else:
            if not isinstance(candidate_str, str):
                raise ValueError("Invalid candidate")
            candidate = candidate_from_sdp(candidate_str.removeprefix("candidate:"))
            mid, index = data.get('sdpMid'), data.get('sdpMLineIndex')
            if mid is not None and not isinstance(mid, str):
                raise ValueError("Invalid sdpMid")
            if index is not None and (type(index) is not int or index < 0):
                raise ValueError("Invalid sdpMLineIndex")
            if mid is None and index is None:
                raise ValueError("Missing candidate media index")
            candidate.sdpMid, candidate.sdpMLineIndex = mid, index
        if not pc.remote_description_ready:
            if len(pc.pending_candidates) >= MAX_PENDING_CANDIDATES:
                raise ValueError("Too many pending candidates")
            pc.pending_candidates.append(candidate)
        else:
            await pc.addIceCandidate(candidate)
    except Exception:
        info(f"Invalid ICE candidate for {sid}")
        socketio.emit("signaling_error", {"error": "无效 ICE 候选"}, room=sid)


@socketio.on('ice_candidate')
def handle_ice_candidate(data):
    sid = request.sid
    if not isinstance(data, dict):
        socketio.emit("signaling_error", {"error": "无效 ICE 消息"}, room=sid)
        return
    with pc_lock:
        pc = peer_connections.get(sid)
    if pc is not None:
        future = asyncio.run_coroutine_threadsafe(process_candidate(sid, pc, data), async_loop)
        future.add_done_callback(report_task_error)


@socketio.on('disconnect')
def handle_disconnect(reason=None):
    """Retire signaling without blocking or interrupting an active P2P transfer."""
    sid = request.sid
    info(f"[WebRTC] Signaling disconnected for {sid}: {reason}")
    with pc_lock:
        pc = peer_connections.get(sid)
    if pc is not None:
        info(
            f"Peer {sid}: signaling disconnected, reason={reason}, connection={pc.connectionState}, "
            f"ICE={pc.iceConnectionState}, active transfers={len(pc.media_tasks)}"
        )
        future = asyncio.run_coroutine_threadsafe(retire_peer(sid, pc), async_loop)
        future.add_done_callback(report_task_error)


# ══════════════════════════════════════════════════════════════
#  Asyncio Bridge — runs aiortc's event loop in a background thread
# ══════════════════════════════════════════════════════════════

def async_exception_handler(loop, context):
    """Custom exception handler for the asyncio event loop.

    aiortc's internal tasks can raise exceptions that are never awaited,
    causing Python to print 'Task exception was never retrieved' warnings.
    Known benign cases:
      - ConnectionError from RTCSctpTransport._transmit (ICE dropped)
      - InvalidStateError from RTCPeerConnection.__connect (ICE closed)
    We suppress these and only forward truly unexpected errors.
    """
    exception = context.get('exception')
    message = context.get('message', '')
    info(f"[Async] Exception: {exception}, Message: {message}")
    # Suppress known benign aiortc warnings
    if isinstance(exception, (ConnectionError, InvalidStateError)):
        return

    if 'Task exception was never retrieved' in message:
        if isinstance(exception, (ConnectionError, InvalidStateError)):
            return
        # Don't print these during normal operation
        return

    # Suppress "Task was destroyed but it is pending!" warnings from aiortc internals
    if 'Task was destroyed but it is pending' in message:
        return

    loop.default_exception_handler(context)


def start_async_loop():
    """Start a dedicated asyncio event loop in a daemon thread for aiortc."""
    global async_loop
    async_loop = asyncio.new_event_loop()
    async_loop.set_exception_handler(async_exception_handler)
    asyncio.set_event_loop(async_loop)
    loop_ready.set()

    try:
        async_loop.run_forever()
    finally:
        try:
            # Give aiortc's internal tasks time to clean up gracefully
            pending = asyncio.all_tasks(async_loop)
            if pending:
                info(f"[Async] Cancelling {len(pending)} pending tasks...")
                for task in pending:
                    task.cancel()

                # Allow tasks to handle cancellation properly
                async_loop.run_until_complete(
                    asyncio.wait(pending, timeout=CLOSE_TIMEOUT)
                )

                # Second pass: cancel any tasks that spawned during cleanup
                remaining = asyncio.all_tasks(async_loop)
                if remaining:
                    for task in remaining:
                        task.cancel()
                    async_loop.run_until_complete(
                        asyncio.wait(remaining, timeout=CLOSE_TIMEOUT)
                    )
        except Exception as e:
            info(f"[Async] Error while canceling pending tasks: {e}")
        finally:
            async_loop.close()
            info("[Async] Event loop closed gracefully.")


# ══════════════════════════════════════════════════════════════
#  Server Startup
# ══════════════════════════════════════════════════════════════

def setup_media_server(port=9000):
    """Start the Flask + SocketIO media server."""
    os.chdir(get_current_directory())

    info("Press Ctrl+C to stop the server")

    socketio.run(
        app,
        host='0.0.0.0',
        port=port,
        debug=False,
        allow_unsafe_werkzeug=True,
    )


async def shutdown_peers():
    with pc_lock:
        peers = list(peer_connections.items())
    tasks = [asyncio.create_task(close_peer(sid, pc)) for sid, pc in peers]
    for task in tasks:
        task.add_done_callback(report_task_error)
    if tasks:
        _, pending = await asyncio.wait(tasks, timeout=CLOSE_TIMEOUT * 2 + 1)
        if pending:
            info(f"{len(pending)} peer cleanups still pending at shutdown")
    if background_file_tasks:
        _, pending = await asyncio.wait(tuple(background_file_tasks), timeout=CLOSE_TIMEOUT)
        if pending:
            info(f"{len(pending)} file operations still pending at shutdown")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    stop_event = threading.Event()
    loop_ready.clear()
    loop_thread = threading.Thread(target=start_async_loop, daemon=True)
    frp_thread = None
    loop_thread.start()
    try:
        if not loop_ready.wait(timeout=10):
            raise RuntimeError("Asyncio loop failed to start")
        frp_thread = setup_frp(port=9000, stop_event=stop_event)
        setup_media_server(port=9000)
    finally:
        stop_event.set()
        if async_loop is not None and async_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(shutdown_peers(), async_loop)
            future.add_done_callback(report_task_error)
            try:
                future.result(timeout=CLOSE_TIMEOUT * 3 + 2)
            except Exception:
                info("Shutdown did not finish within its deadline")
            async_loop.call_soon_threadsafe(async_loop.stop)
        loop_thread.join(timeout=CLOSE_TIMEOUT * 2 + 1)
        if frp_thread is not None:
            frp_thread.join(timeout=12)
            if frp_thread.is_alive():
                info("FRP thread did not stop within its deadline")


if __name__ == '__main__':
    main()