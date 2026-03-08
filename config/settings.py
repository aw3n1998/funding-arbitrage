"""
全局配置文件
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass
class ExchangeConfig:
    """单个交易所配置"""
    name: str
    api_key: str
    api_secret: str
    passphrase: Optional[str] = None   # Gate/Bitget/OKX 需要
    sandbox: bool = False


@dataclass
class ArbitrageConfig:
    """套利策略配置"""
    # 最小年化收益率阈值（0.01 = 1%）
    min_annual_rate_diff: float = 0.05
    # 单次套利最大仓位 USDT
    max_position_usdt: float = 500.0
    # 最小仓位 USDT
    min_position_usdt: float = 50.0
    # 杠杆倍数
    leverage: int = 1
    # 平仓触发条件：费率差低于此值时平仓（年化）
    close_rate_threshold: float = 0.01
    # 最大同时持有套利对数
    max_concurrent_positions: int = 5
    # 监控的交易对白名单（空表示监控所有）
    symbol_whitelist: List[str] = field(default_factory=lambda: [
        "BTC/USDT:USDT",
        "ETH/USDT:USDT",
        "SOL/USDT:USDT",
        "BNB/USDT:USDT",
        "XRP/USDT:USDT",
        "DOGE/USDT:USDT",
        "ADA/USDT:USDT",
        "AVAX/USDT:USDT",
    ])
    # 滑点容忍（0.001 = 0.1%）
    slippage_tolerance: float = 0.001
    # 单边手续费率（保守估计）
    taker_fee_rate: float = 0.0005


@dataclass
class MonitorConfig:
    """监控配置"""
    # 费率刷新间隔（秒）
    rate_refresh_interval: int = 10
    # 持仓状态刷新间隔（秒）
    position_refresh_interval: int = 30
    # 是否开启桌面通知
    enable_notification: bool = True


@dataclass
class AppConfig:
    """应用总配置"""
    exchanges: Dict[str, ExchangeConfig] = field(default_factory=dict)
    arbitrage: ArbitrageConfig = field(default_factory=ArbitrageConfig)
    monitor: MonitorConfig = field(default_factory=MonitorConfig)
    # 是否真实交易（False = 仅监控，不下单）
    live_trading: bool = False
    # 日志级别
    log_level: str = "INFO"


def load_config() -> AppConfig:
    """从环境变量加载配置"""
    config = AppConfig()

    # 币安
    if os.getenv("BINANCE_API_KEY"):
        config.exchanges["binance"] = ExchangeConfig(
            name="binance",
            api_key=os.getenv("BINANCE_API_KEY", ""),
            api_secret=os.getenv("BINANCE_API_SECRET", ""),
        )

    # 欧意 OKX
    if os.getenv("OKX_API_KEY"):
        config.exchanges["okx"] = ExchangeConfig(
            name="okx",
            api_key=os.getenv("OKX_API_KEY", ""),
            api_secret=os.getenv("OKX_API_SECRET", ""),
            passphrase=os.getenv("OKX_PASSPHRASE", ""),
        )

    # Gate
    if os.getenv("GATE_API_KEY"):
        config.exchanges["gate"] = ExchangeConfig(
            name="gateio",
            api_key=os.getenv("GATE_API_KEY", ""),
            api_secret=os.getenv("GATE_API_SECRET", ""),
        )

    # Bitget
    if os.getenv("BITGET_API_KEY"):
        config.exchanges["bitget"] = ExchangeConfig(
            name="bitget",
            api_key=os.getenv("BITGET_API_KEY", ""),
            api_secret=os.getenv("BITGET_API_SECRET", ""),
            passphrase=os.getenv("BITGET_PASSPHRASE", ""),
        )

    # 套利参数
    config.arbitrage.min_annual_rate_diff = float(
        os.getenv("MIN_ANNUAL_RATE_DIFF", "0.05"))
    config.arbitrage.max_position_usdt = float(
        os.getenv("MAX_POSITION_USDT", "500"))
    config.arbitrage.leverage = int(os.getenv("LEVERAGE", "1"))
    config.arbitrage.max_concurrent_positions = int(
        os.getenv("MAX_CONCURRENT_POSITIONS", "5"))

    config.live_trading = os.getenv("LIVE_TRADING", "false").lower() == "true"
    config.log_level = os.getenv("LOG_LEVEL", "INFO")

    return config
