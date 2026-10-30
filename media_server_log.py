"""
media_server_log.py — 线程安全的日志模块

特性：
  - 自动生成 media-server-log-[时间戳].log 文件名
  - 基于标准 logging 模块，内置线程安全（Handler 级锁）
  - 同时输出到控制台和日志文件
  - 提供 log_print() 替代 print()，将输出同步写入日志
"""

import logging
import os
import sys
import threading
from datetime import datetime

from media_file_util import get_current_directory

_logger = None
_log_filename = None
_init_lock = threading.Lock()

def build_test_filename():
    filename = os.path.join(get_current_directory(), "log_out")
    return filename

def build_log_filename():
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"media-server-log-{timestamp}.log"
    filename = os.path.join(get_current_directory(), filename)
    return filename

def setup_logging(console_level=logging.INFO):
    """初始化日志系统，返回 (logger, log_filename)。

    只需在主线程启动时调用一次，后续通过 get_logger() 获取同一个 logger 实例。
    重复调用是安全的（幂等）。
    """
    global _logger, _log_filename

    with _init_lock:
        if _logger is not None:
            return _logger, _log_filename

        _log_filename = build_log_filename()

        _logger = logging.getLogger("media_server")
        _logger.setLevel(logging.DEBUG)
        _logger.propagate = False

        # 文件处理器 — 所有级别都写入文件
        file_handler = logging.FileHandler(_log_filename, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_fmt = logging.Formatter(
            "%(asctime)s.%(msecs)03d [%(threadName)s] %(levelname)-7s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_fmt)
        _logger.addHandler(file_handler)

        # 控制台处理器 — 默认 INFO 及以上输出到终端
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(console_level)
        console_fmt = logging.Formatter(
            "%(asctime)s [%(threadName)s] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler.setFormatter(console_fmt)
        _logger.addHandler(console_handler)

    return _logger, _log_filename


def get_logger():
    """获取已初始化的 logger 实例。如果未初始化则自动初始化。"""
    global _logger
    if _logger is None:
        setup_logging()
    return _logger


def get_log_filename():
    """返回当前日志文件的完整路径。"""
    global _log_filename
    if _log_filename is None:
        setup_logging()
    return _log_filename


def log_print(*args, sep=" ", end="\n", level=logging.INFO):
    if not os.path.exists(build_test_filename()):
        return

    message = sep.join(str(arg) for arg in args)
    logger = get_logger()
    logger.log(level, message)
    if end != "\n":
        print(message, end=end, flush=True)


# 便捷别名
debug = lambda *a, **kw: log_print(*a, level=logging.DEBUG, **kw)
info = lambda *a, **kw: log_print(*a, level=logging.INFO, **kw)
warning = lambda *a, **kw: log_print(*a, level=logging.WARNING, **kw)
error = lambda *a, **kw: log_print(*a, level=logging.ERROR, **kw)