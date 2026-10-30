"""Local compatibility fixes for aioice's STUN transaction lifecycle."""

from functools import wraps
import sys

from aioice import stun
from aioice.ice import StunProtocol


def install_windows_udp_close_guard():
    """Avoid Python's Proactor UDP close hang after ICE shutdown.

    Python 3.12 can leave connection_lost() pending forever when closing a
    UDP transport with outstanding sends (CPython issue #156920). At this
    point the ICE connection is being discarded, so aborting its datagrams
    is safe and still delivers connection_lost() to aioice. This also covers
    consent expiry, which closes aioice before our peer cleanup runs.
    """
    if sys.platform != 'win32':
        return
    from asyncio.proactor_events import _ProactorDatagramTransport

    close = StunProtocol.close
    if getattr(close, '_mediabro_guarded', False):
        return

    @wraps(close)
    async def guarded_close(protocol):
        if isinstance(protocol.transport, _ProactorDatagramTransport):
            protocol.transport.abort()
        await close(protocol)

    guarded_close._mediabro_guarded = True
    StunProtocol.close = guarded_close


def install_stun_retry_guard():
    """Ignore stale retries after completion/cancellation (aioice 0.10.2).

    A ready timer can run before Transaction.run() resumes and cancels it in
    its finally block. Upstream's retry unconditionally sets an exception on
    the Future at exhaustion, even when that Future is already done:
    https://github.com/aiortc/aioice/blob/0.10.2/src/aioice/stun.py

    Keep the private API workaround here; pending transactions retain the
    upstream retry policy and real transport/timeout errors still propagate.
    """
    retry = stun.Transaction._Transaction__retry
    if getattr(retry, "_mediabro_guarded", False):
        return

    @wraps(retry)
    def guarded_retry(transaction):
        if transaction._Transaction__future.done():
            return
        return retry(transaction)

    guarded_retry._mediabro_guarded = True
    stun.Transaction._Transaction__retry = guarded_retry
