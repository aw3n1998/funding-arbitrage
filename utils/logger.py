"""
日志工具
"""
import sys
from loguru import logger
from pathlib import Path


def setup_logger(log_level: str = "INFO", log_dir: str = "logs") -> None:
    """初始化日志系统"""
    Path(log_dir).mkdir(exist_ok=True)

    logger.remove()

    # 控制台输出（带颜色）
    logger.add(
        sys.stdout,
        level=log_level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
    )

    # 文件输出（按日期滚动）
    logger.add(
        f"{log_dir}/arbitrage_{{time:YYYY-MM-DD}}.log",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} | {message}",
        rotation="00:00",
        retention="30 days",
        encoding="utf-8",
    )

    # 独立错误日志
    logger.add(
        f"{log_dir}/error.log",
        level="ERROR",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} | {message}",
        rotation="10 MB",
        retention="60 days",
        encoding="utf-8",
    )
