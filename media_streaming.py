"""Remux local H.264/AAC MP4 into incremental MSE input without transcoding."""

import asyncio
import os
import shutil
import subprocess

import av


def stream_metadata(filepath):
    with av.open(str(filepath)) as container:
        video = next(iter(container.streams.video), None)
        audio = next(iter(container.streams.audio), None)
        if video is None or video.codec_context.name != 'h264':
            raise ValueError('边下边播目前需要 H.264 视频')
        extra = video.codec_context.extradata or b''
        if len(extra) < 4 or extra[0] != 1:
            raise ValueError('无效的 H.264 MP4 配置')
        codecs = ['avc1.' + extra[1:4].hex()]
        if audio is not None:
            config = audio.codec_context.extradata or b''
            if audio.codec_context.name != 'aac' or not config or config[0] >> 3 != 2:
                raise ValueError('边下边播目前需要 AAC-LC 音频')
            codecs.append('mp4a.40.2')
        return {
            'type': 'meta', 'mode': 'mse', 'name': os.path.basename(filepath),
            'mime': 'video/mp4; codecs="' + ','.join(codecs) + '"',
            'duration': (container.duration or 0) / av.time_base,
        }


async def fragmented_mp4(filepath):
    executable = os.environ.get('MEDIA_FFMPEG') or shutil.which('ffmpeg')
    if not executable:
        raise RuntimeError('边下边播需要安装 FFmpeg，或配置 MEDIA_FFMPEG 路径')
    creation = asyncio.create_task(asyncio.create_subprocess_exec(
        executable, '-hide_banner', '-loglevel', 'error', '-nostdin',
        '-i', str(filepath), '-map', '0:v:0', '-map', '0:a:0?',
        '-c', 'copy', '-movflags', '+empty_moov+default_base_moof+frag_keyframe',
        '-frag_duration', '1000000', '-avoid_negative_ts', 'make_zero',
        '-flush_packets', '1', '-f', 'mp4', 'pipe:1',
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        **({'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {}),
    ))
    process = None
    stderr_task = None
    errors = bytearray()

    async def drain_errors():
        while chunk := await process.stderr.read(4096):
            errors.extend(chunk)
            del errors[:-8192]

    try:
        try:
            process = await asyncio.shield(creation)
        except asyncio.CancelledError:
            # Process creation may finish after cancellation; still reap it.
            process = await creation
            raise
        stderr_task = asyncio.create_task(drain_errors())
        while chunk := await asyncio.wait_for(process.stdout.read(64 * 1024), 60):
            yield chunk
        code = await asyncio.wait_for(process.wait(), 5)
        await stderr_task
        if code:
            raise RuntimeError('FFmpeg 分片失败: ' + errors.decode('utf-8', errors='replace'))
    finally:
        if process is not None and process.returncode is None:
            async def discard_output():
                while await process.stdout.read(64 * 1024):
                    pass

            # A paused stdout pipe can otherwise keep Process.wait() pending
            # even after the OS process has exited.
            discard_task = asyncio.create_task(discard_output())
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(process.wait(), 2)
            except asyncio.TimeoutError:
                if process.returncode is None:
                    process.kill()
                await process.wait()
            await discard_task
        if stderr_task is not None:
            await stderr_task
