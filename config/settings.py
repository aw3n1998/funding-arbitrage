"""
PO3/AMD 剥头皮策略 — 全局配置（仅支持 Bitget）
"""
from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass
class PO3Config:
    # ── Bitget 连接（无 exchange 选择器，固定为 Bitget）──
    api_key: str = ""
    api_secret: str = ""
    api_passphrase: str = ""      # Bitget 必填 passphrase
    testnet: bool = True
    symbol: str = "BTC/USDT:USDT"
    leverage: int = 30             # 建议 25~40

    # ── 保证金模式 ──
    margin_mode: str = "isolated"

    # ── 风险控制 ──
    risk_per_trade: float = 0.01   # 每笔风险占账户净值比例 (1%)
    max_daily_trades: int = 10     # 每日最大交易次数
    max_daily_loss: float = 0.06   # 每日最大亏损比例 (6%)

    # ── 止盈止损 ──
    tp1_rr: float = 2.2            # 第一目标 RR（平仓 tp1_close_pct 比例）
    tp2_rr: float = 3.0            # 第二目标 RR（trailing stop 跟踪剩余仓位）
    tp1_close_pct: float = 0.5     # 到达 TP1 时平仓 50%
    sl_atr_buffer: float = 0.2     # SL 在 manipulation 极值外侧 ATR*0.2

    # ── PO3 检测参数 ──
    acc_bars: int = 10             # 累积阶段识别所需最小 K 线数
    acc_atr_mult: float = 1.5      # 累积区间高度需 < ATR(14) * 此值
    manip_atr_mult: float = 0.5    # 假突破需超出 range 边界 ATR*此值
    manip_max_age_bars: int = 8    # Manipulation 有效窗口（15m K 线根数）

    # ── Trailing Stop ──
    trailing_atr_mult: float = 1.5  # Trailing stop 距离 = ATR * 此值

    # ── 事件循环间隔（秒）──
    poll_interval_1m: int = 5      # Trailing stop tick 间隔（秒）

    # ── 日志 ──
    log_level: str = "INFO"


def load_config() -> PO3Config:
    """从环境变量加载配置"""
    return PO3Config(
        api_key=os.getenv("PO3_API_KEY", ""),
        api_secret=os.getenv("PO3_API_SECRET", ""),
        api_passphrase=os.getenv("PO3_API_PASSPHRASE", ""),
        testnet=os.getenv("PO3_TESTNET", "true").lower() == "true",
        symbol=os.getenv("PO3_SYMBOL", "BTC/USDT:USDT"),
        leverage=int(os.getenv("PO3_LEVERAGE", "30")),
        margin_mode=os.getenv("PO3_MARGIN_MODE", "isolated"),
        risk_per_trade=float(os.getenv("PO3_RISK_PER_TRADE", "0.01")),
        max_daily_trades=int(os.getenv("PO3_MAX_DAILY_TRADES", "10")),
        max_daily_loss=float(os.getenv("PO3_MAX_DAILY_LOSS", "0.06")),
        tp1_rr=float(os.getenv("PO3_TP1_RR", "2.2")),
        tp2_rr=float(os.getenv("PO3_TP2_RR", "3.0")),
        tp1_close_pct=float(os.getenv("PO3_TP1_CLOSE_PCT", "0.5")),
        sl_atr_buffer=float(os.getenv("PO3_SL_ATR_BUFFER", "0.2")),
        acc_bars=int(os.getenv("PO3_ACC_BARS", "10")),
        acc_atr_mult=float(os.getenv("PO3_ACC_ATR_MULT", "1.5")),
        manip_atr_mult=float(os.getenv("PO3_MANIP_ATR_MULT", "0.5")),
        manip_max_age_bars=int(os.getenv("PO3_MANIP_MAX_AGE_BARS", "8")),
        trailing_atr_mult=float(os.getenv("PO3_TRAILING_ATR_MULT", "1.5")),
        poll_interval_1m=int(os.getenv("PO3_POLL_1M", "5")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
