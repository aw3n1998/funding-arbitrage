"""
PO3/AMD 三阶段检测器
Accumulation → Manipulation → Distribution

15m 图：识别阶段和 Bias 方向
1m 图：识别具体入场信号（吞没/Pinbar/FVG回测）
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import pandas_ta as ta
from loguru import logger


# ─────────────────────────────── 数据结构 ────────────────────────────────


@dataclass
class AccumulationRange:
    """累积区间"""
    high: float
    low: float
    atr: float
    bar_count: int          # 参与识别的 K 线数量
    detected_at: datetime = field(default_factory=datetime.now)

    @property
    def height(self) -> float:
        return self.high - self.low

    def __repr__(self) -> str:
        return (
            f"Accumulation [H:{self.high:.2f} L:{self.low:.2f} "
            f"高度:{self.height:.2f} ATR:{self.atr:.2f}]"
        )


@dataclass
class ManipulationEvent:
    """Manipulation 假突破事件"""
    direction: str              # "up"（扫高）| "down"（扫低）
    extreme: float              # 假突破极值（SL 放在外侧）
    bias: str                   # "bullish"（distribution向上）| "bearish"（向下）
    acc_range: AccumulationRange
    timestamp: datetime = field(default_factory=datetime.now)
    fvg_high: float = 0.0       # FVG 上边界
    fvg_low: float = 0.0        # FVG 下边界

    @property
    def has_fvg(self) -> bool:
        return self.fvg_high > 0 and self.fvg_low > 0

    def __repr__(self) -> str:
        fvg_str = f" FVG[{self.fvg_low:.2f}~{self.fvg_high:.2f}]" if self.has_fvg else ""
        return (
            f"Manipulation dir={self.direction} extreme={self.extreme:.2f} "
            f"bias={self.bias}{fvg_str}"
        )


@dataclass
class EntrySignal:
    """1m 图入场信号"""
    direction: str              # "long" | "short"
    entry_price: float          # 信号触发时价格（市价参考）
    stop_loss: float            # 建议止损价
    manipulation: ManipulationEvent
    signal_type: str            # "engulfing" | "pinbar" | "fvg_retest" | "bos"
    timestamp: datetime = field(default_factory=datetime.now)

    def __repr__(self) -> str:
        return (
            f"EntrySignal {self.direction.upper()} @ {self.entry_price:.2f} "
            f"SL:{self.stop_loss:.2f} type={self.signal_type}"
        )


# ─────────────────────────────── 检测器 ──────────────────────────────────


class PO3Detector:
    """
    PO3/AMD 三阶段识别器

    用法:
        detector = PO3Detector(config)
        acc = detector.detect_accumulation(df_15m)
        if acc:
            manip = detector.detect_manipulation(df_15m, acc)
            if manip:
                signal = detector.detect_entry_signal(df_1m, manip)
    """

    def __init__(self, config):
        self.cfg = config

    # ──────────────────── 1. Accumulation ─────────────────────

    def detect_accumulation(self, df: pd.DataFrame) -> Optional[AccumulationRange]:
        """
        在 15m K线中寻找最近的累积区间。

        条件：
        - 最近 acc_bars 根 K 线的高低点差 < ATR(14) * acc_atr_mult
        - 窗口内没有强势趋势（收盘价极差 < ATR * 1.0）
        """
        if len(df) < 20:
            return None

        atr_series = ta.atr(df["high"], df["low"], df["close"], length=14)
        if atr_series is None or atr_series.isna().all():
            return None

        atr = float(atr_series.iloc[-1])
        if atr <= 0:
            return None

        n = self.cfg.acc_bars
        # 从最近开始向前滑动查找最新的累积窗口
        for end in range(len(df), n, -1):
            window = df.iloc[end - n: end]
            rng_high = float(window["high"].max())
            rng_low = float(window["low"].min())
            height = rng_high - rng_low

            # 区间高度条件
            if height >= atr * self.cfg.acc_atr_mult:
                continue
            # 没有强趋势：收盘价极差也要小
            close_range = float(window["close"].max() - window["close"].min())
            if close_range >= atr * 1.0:
                continue

            logger.debug(
                f"[ACC] 发现累积区间 H:{rng_high:.2f} L:{rng_low:.2f} "
                f"高度:{height:.2f} ATR:{atr:.2f}"
            )
            return AccumulationRange(
                high=rng_high,
                low=rng_low,
                atr=atr,
                bar_count=n,
            )

        return None

    # ──────────────────── 2. Manipulation ─────────────────────

    def detect_manipulation(
        self, df: pd.DataFrame, acc: AccumulationRange
    ) -> Optional[ManipulationEvent]:
        """
        检测累积区间之后的 Manipulation 假突破。

        条件（检查最近 3 根 K 线）：
        - wick 超出区间边界 ATR*manip_atr_mult 以上
        - 但 收盘价 回到区间内（或之后一根也回到区间内）
        - 记录极值、判断 bias
        """
        if len(df) < 5:
            return None

        atr = acc.atr
        threshold = atr * self.cfg.manip_atr_mult

        # 检查倒数第2、3根K线（倒数第1根是正在形成的）
        for i in range(-2, -5, -1):
            try:
                candle = df.iloc[i]
            except IndexError:
                break

            # 向上假突破：高点超出 acc.high，但收盘回到 acc.high 以下
            if (candle["high"] > acc.high + threshold
                    and candle["close"] < acc.high):
                fvg = self._find_fvg(df, "bearish")
                event = ManipulationEvent(
                    direction="up",
                    extreme=float(candle["high"]),
                    bias="bearish",
                    acc_range=acc,
                    fvg_high=fvg[1] if fvg else 0.0,
                    fvg_low=fvg[0] if fvg else 0.0,
                )
                logger.debug(f"[MANIP] {event}")
                return event

            # 向下假突破：低点跌破 acc.low，但收盘回到 acc.low 以上
            if (candle["low"] < acc.low - threshold
                    and candle["close"] > acc.low):
                fvg = self._find_fvg(df, "bullish")
                event = ManipulationEvent(
                    direction="down",
                    extreme=float(candle["low"]),
                    bias="bullish",
                    acc_range=acc,
                    fvg_high=fvg[1] if fvg else 0.0,
                    fvg_low=fvg[0] if fvg else 0.0,
                )
                logger.debug(f"[MANIP] {event}")
                return event

        return None

    # ──────────────────── 3. FVG 识别 ─────────────────────────

    def _find_fvg(
        self, df: pd.DataFrame, direction: str
    ) -> Optional[Tuple[float, float]]:
        """
        在最近 10 根 K 线中寻找 Fair Value Gap。

        看涨 FVG：candle[i].high < candle[i+2].low  → 价格gap在上方
        看跌 FVG：candle[i].low  > candle[i+2].high → 价格gap在下方

        返回 (fvg_low, fvg_high) 或 None
        """
        window = df.iloc[-10:]
        bars = list(window.itertuples())

        for i in range(len(bars) - 2):
            c0, c1, c2 = bars[i], bars[i + 1], bars[i + 2]
            if direction == "bullish":
                if c0.high < c2.low:
                    gap_low = float(c0.high)
                    gap_high = float(c2.low)
                    logger.debug(f"[FVG 多] {gap_low:.2f}~{gap_high:.2f}")
                    return (gap_low, gap_high)
            else:
                if c0.low > c2.high:
                    gap_low = float(c2.high)
                    gap_high = float(c0.low)
                    logger.debug(f"[FVG 空] {gap_low:.2f}~{gap_high:.2f}")
                    return (gap_low, gap_high)

        return None

    # ──────────────────── 4. Entry Signal (1m) ─────────────────

    def detect_entry_signal(
        self, df_1m: pd.DataFrame, manip: ManipulationEvent
    ) -> Optional[EntrySignal]:
        """
        在 1m K 线中检测 Manipulation 后的入场信号。

        支持三种信号类型（按优先级）：
        1. FVG retest     — 价格回踩 FVG 区域
        2. Engulfing      — 吞没烛（收盘吞噬前一根实体）
        3. Pinbar         — 长影线蜡烛（影线 > 实体 * 2）
        """
        if len(df_1m) < 3:
            return None

        direction = "long" if manip.bias == "bullish" else "short"
        last = df_1m.iloc[-1]
        prev = df_1m.iloc[-2]

        # ── FVG retest（最高优先级，最精确入场点）──
        if manip.has_fvg:
            signal = self._check_fvg_retest(last, manip, direction)
            if signal:
                return signal

        # ── 吞没烛 ──
        signal = self._check_engulfing(prev, last, direction, manip)
        if signal:
            return signal

        # ── Pinbar ──
        signal = self._check_pinbar(last, direction, manip)
        if signal:
            return signal

        return None

    def _check_fvg_retest(
        self, candle, manip: ManipulationEvent, direction: str
    ) -> Optional[EntrySignal]:
        """价格回踩 FVG 区域并出现反转迹象"""
        fvg_low = manip.fvg_low
        fvg_high = manip.fvg_high
        close = float(candle.close)
        low = float(candle.low)
        high = float(candle.high)

        if direction == "long":
            # 价格低点触及 FVG 区间，但收盘在 FVG 内或之上
            if low <= fvg_high and close >= fvg_low:
                entry = close
                sl = manip.extreme - manip.acc_range.atr * self.cfg.sl_atr_buffer
                logger.info(f"[SIGNAL] FVG retest LONG @ {entry:.2f}")
                return EntrySignal(
                    direction="long", entry_price=entry,
                    stop_loss=sl, manipulation=manip,
                    signal_type="fvg_retest",
                )
        else:
            # 价格高点触及 FVG 区间，但收盘在 FVG 内或之下
            if high >= fvg_low and close <= fvg_high:
                entry = close
                sl = manip.extreme + manip.acc_range.atr * self.cfg.sl_atr_buffer
                logger.info(f"[SIGNAL] FVG retest SHORT @ {entry:.2f}")
                return EntrySignal(
                    direction="short", entry_price=entry,
                    stop_loss=sl, manipulation=manip,
                    signal_type="fvg_retest",
                )
        return None

    def _check_engulfing(
        self, prev, last, direction: str, manip: ManipulationEvent
    ) -> Optional[EntrySignal]:
        """看涨/看跌吞没烛：当前实体完全吞噬前一根实体"""
        prev_body_top = max(float(prev.open), float(prev.close))
        prev_body_bot = min(float(prev.open), float(prev.close))
        last_body_top = max(float(last.open), float(last.close))
        last_body_bot = min(float(last.open), float(last.close))

        if direction == "long":
            # 前一根为阴线，当前为阳线且完全吞噬
            prev_bearish = float(prev.close) < float(prev.open)
            last_bullish = float(last.close) > float(last.open)
            if (prev_bearish and last_bullish
                    and last_body_bot <= prev_body_bot
                    and last_body_top >= prev_body_top):
                entry = float(last.close)
                sl = manip.extreme - manip.acc_range.atr * self.cfg.sl_atr_buffer
                logger.info(f"[SIGNAL] Engulfing LONG @ {entry:.2f}")
                return EntrySignal(
                    direction="long", entry_price=entry,
                    stop_loss=sl, manipulation=manip,
                    signal_type="engulfing",
                )
        else:
            # 前一根为阳线，当前为阴线且完全吞噬
            prev_bullish = float(prev.close) > float(prev.open)
            last_bearish = float(last.close) < float(last.open)
            if (prev_bullish and last_bearish
                    and last_body_top >= prev_body_top
                    and last_body_bot <= prev_body_bot):
                entry = float(last.close)
                sl = manip.extreme + manip.acc_range.atr * self.cfg.sl_atr_buffer
                logger.info(f"[SIGNAL] Engulfing SHORT @ {entry:.2f}")
                return EntrySignal(
                    direction="short", entry_price=entry,
                    stop_loss=sl, manipulation=manip,
                    signal_type="engulfing",
                )
        return None

    def _check_pinbar(
        self, candle, direction: str, manip: ManipulationEvent
    ) -> Optional[EntrySignal]:
        """
        Pinbar：影线长度 >= 实体长度 * 2，且收盘偏向实体一侧。

        看涨 Pinbar：长下影线，收盘靠近高点
        看跌 Pinbar：长上影线，收盘靠近低点
        """
        open_ = float(candle.open)
        close = float(candle.close)
        high = float(candle.high)
        low = float(candle.low)

        body = abs(close - open_)
        upper_wick = high - max(open_, close)
        lower_wick = min(open_, close) - low

        if body == 0:
            return None

        if direction == "long":
            # 长下影线
            if lower_wick >= body * 2 and close > open_:
                entry = close
                sl = manip.extreme - manip.acc_range.atr * self.cfg.sl_atr_buffer
                logger.info(f"[SIGNAL] Pinbar LONG @ {entry:.2f}")
                return EntrySignal(
                    direction="long", entry_price=entry,
                    stop_loss=sl, manipulation=manip,
                    signal_type="pinbar",
                )
        else:
            # 长上影线
            if upper_wick >= body * 2 and close < open_:
                entry = close
                sl = manip.extreme + manip.acc_range.atr * self.cfg.sl_atr_buffer
                logger.info(f"[SIGNAL] Pinbar SHORT @ {entry:.2f}")
                return EntrySignal(
                    direction="short", entry_price=entry,
                    stop_loss=sl, manipulation=manip,
                    signal_type="pinbar",
                )
        return None

    # ──────────────────── 工具 ────────────────────────────────

    @staticmethod
    def candles_to_df(ohlcv: list) -> pd.DataFrame:
        """将 CCXT ohlcv 列表转换为 DataFrame"""
        df = pd.DataFrame(
            ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df = df.set_index("timestamp")
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        return df

    @staticmethod
    def get_current_atr(df: pd.DataFrame, length: int = 14) -> float:
        """获取最新 ATR 值"""
        atr_series = ta.atr(df["high"], df["low"], df["close"], length=length)
        if atr_series is None or atr_series.isna().all():
            return 0.0
        return float(atr_series.iloc[-1])
