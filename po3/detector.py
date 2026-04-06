"""
PO3/AMD 三阶段检测器
Accumulation → Manipulation → Distribution

15m 图：识别阶段和 Bias 方向（事件驱动，只处理已收盘 K 线）
1m  图：识别具体入场信号（吞没 / Pinbar / FVG 回测）

修复记录：
  - [BUG1]  Accumulation 不再扫描历史，只检测最近 acc_bars 根 K 线
  - [BUG6]  close_range 阈值从 1.0 ATR 放宽至 1.5 ATR
  - [BUG7]  FVG 从 detect_manipulation 中移除，改在 detect_entry_signal
             中用 1m 数据寻找，避免时间框架混用
  - [BUG13] Pinbar 允许阴线 Hammer（去掉 close > open_ 强制要求）
  - [BUG14] 信号去重：记录上次触发信号的 K 线时间戳，同根蜡烛不重复触发
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
    """累积区间（基于最近 N 根已收盘 K 线）"""
    high: float
    low: float
    atr: float
    bar_count: int
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
    direction: str          # "up"（扫高）| "down"（扫低）
    extreme: float          # 假突破极值（SL 放在外侧）
    bias: str               # "bullish" | "bearish"（distribution 方向）
    acc_range: AccumulationRange
    timestamp: datetime = field(default_factory=datetime.now)
    # FVG 字段保留结构，由 detect_entry_signal 在 1m 数据上填充
    fvg_high: float = 0.0
    fvg_low: float = 0.0

    @property
    def has_fvg(self) -> bool:
        return self.fvg_high > 0 and self.fvg_low > 0

    def fingerprint(self) -> tuple:
        """用于去重判断同一个 Manipulation 是否已处理"""
        return (
            self.direction,
            round(self.extreme, 0),
            round(self.acc_range.high, 0),
            round(self.acc_range.low, 0),
        )

    def __repr__(self) -> str:
        fvg_str = f" FVG[{self.fvg_low:.2f}~{self.fvg_high:.2f}]" if self.has_fvg else ""
        return (
            f"Manipulation dir={self.direction} extreme={self.extreme:.2f} "
            f"bias={self.bias}{fvg_str}"
        )


@dataclass
class EntrySignal:
    """1m 图入场信号"""
    direction: str          # "long" | "short"
    entry_price: float
    stop_loss: float
    manipulation: ManipulationEvent
    signal_type: str        # "engulfing" | "pinbar" | "fvg_retest"
    candle_ts: datetime     # 触发信号的 K 线时间戳（用于去重）
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

    所有方法只接收「已收盘」的 K 线 DataFrame（排除最后一根正在形成的 K 线）。
    调用方负责在传入前用 df.iloc[:-1] 剔除当前未收盘蜡烛。
    """

    def __init__(self, config):
        self.cfg = config
        # [BUG14] 信号去重状态
        self._last_entry_candle_ts: Optional[datetime] = None
        self._last_manip_fingerprint: Optional[tuple] = None

    # ──────────────────── 1. Accumulation ─────────────────────

    def detect_accumulation(self, df: pd.DataFrame) -> Optional[AccumulationRange]:
        """
        [BUG1 修复] 只检测最近 acc_bars 根 K 线是否构成累积区间。
        不再扫描历史窗口，避免把历史横盘误识别为当前累积。

        条件：
        - 最近 acc_bars 根 K 线高低点差 < ATR(14) * acc_atr_mult
        - 收盘价极差 < ATR(14) * 1.5（[BUG6] 从 1.0 放宽至 1.5）
        """
        n = self.cfg.acc_bars
        min_rows = n + 15  # 需要足够的历史来计算 ATR(14)
        if len(df) < min_rows:
            return None

        atr = self.get_current_atr(df)
        if atr <= 0:
            return None

        # 只取最近 n 根已收盘 K 线
        window = df.iloc[-n:]
        rng_high = float(window["high"].max())
        rng_low = float(window["low"].min())
        height = rng_high - rng_low

        # 条件 1：区间高度
        if height >= atr * self.cfg.acc_atr_mult:
            logger.debug(
                f"[ACC] 无累积：高度 {height:.2f} >= ATR*{self.cfg.acc_atr_mult} "
                f"({atr * self.cfg.acc_atr_mult:.2f})"
            )
            return None

        # 条件 2：[BUG6] 收盘价极差（1.5 ATR）
        close_range = float(window["close"].max() - window["close"].min())
        if close_range >= atr * 1.5:
            logger.debug(f"[ACC] 无累积：close 极差 {close_range:.2f} >= ATR*1.5")
            return None

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

    # ──────────────────── 2. Manipulation ─────────────────────

    def detect_manipulation(
        self, df: pd.DataFrame, acc: AccumulationRange
    ) -> Optional[ManipulationEvent]:
        """
        检测累积区间末尾或紧随其后的 Manipulation 假突破。

        检查最近 3 根 K 线：
        - wick 超出区间边界 ATR * manip_atr_mult 以上
        - 收盘价回到区间内

        [BUG7 修复] 不在这里寻找 FVG，FVG 由 detect_entry_signal 在 1m 数据上找。
        """
        if len(df) < 5:
            return None

        atr = acc.atr
        threshold = atr * self.cfg.manip_atr_mult

        # 检查倒数第 1、2、3 根已收盘 K 线
        for i in [-1, -2, -3]:
            try:
                candle = df.iloc[i]
            except IndexError:
                break

            # 向上假突破：高点超出 acc.high + threshold，收盘回到 acc.high 以下
            if (float(candle["high"]) > acc.high + threshold
                    and float(candle["close"]) < acc.high):
                event = ManipulationEvent(
                    direction="up",
                    extreme=float(candle["high"]),
                    bias="bearish",
                    acc_range=acc,
                )
                # 检查是否与上次 Manipulation 重复
                if not self._is_new_manipulation(event):
                    return None
                logger.debug(f"[MANIP] {event}")
                return event

            # 向下假突破：低点跌破 acc.low - threshold，收盘回到 acc.low 以上
            if (float(candle["low"]) < acc.low - threshold
                    and float(candle["close"]) > acc.low):
                event = ManipulationEvent(
                    direction="down",
                    extreme=float(candle["low"]),
                    bias="bullish",
                    acc_range=acc,
                )
                if not self._is_new_manipulation(event):
                    return None
                logger.debug(f"[MANIP] {event}")
                return event

        return None

    def _is_new_manipulation(self, event: ManipulationEvent) -> bool:
        """[BUG9 修复] 同一个 Manipulation 不重复触发"""
        fp = event.fingerprint()
        if fp == self._last_manip_fingerprint:
            logger.debug(f"[MANIP] 重复信号，跳过: {fp}")
            return False
        self._last_manip_fingerprint = fp
        return True

    def reset_manip_fingerprint(self) -> None:
        """放弃当前 Manipulation 时重置（允许下次相同结构重新识别）"""
        self._last_manip_fingerprint = None

    # ──────────────────── 3. FVG 识别（仅 1m 数据）─────────────────────

    def find_fvg_1m(
        self, df_1m: pd.DataFrame, direction: str
    ) -> Optional[Tuple[float, float]]:
        """
        [BUG7 修复] FVG 只在 1m 数据上识别，保证时间框架一致性。

        看涨 FVG：candle[i].high < candle[i+2].low  → 中间有价格 gap
        看跌 FVG：candle[i].low  > candle[i+2].high → 中间有价格 gap

        只扫描最近 20 根 1m K 线。
        返回 (fvg_low, fvg_high) 或 None。
        """
        if len(df_1m) < 3:
            return None

        window = df_1m.iloc[-20:]
        bars = list(window.itertuples())

        # 从最新向前扫描（找最近的 FVG）
        for i in range(len(bars) - 3, -1, -1):
            c0, _c1, c2 = bars[i], bars[i + 1], bars[i + 2]
            if direction == "bullish":
                if float(c0.high) < float(c2.low):
                    gap_low = float(c0.high)
                    gap_high = float(c2.low)
                    logger.debug(f"[FVG 多] {gap_low:.2f}~{gap_high:.2f}")
                    return (gap_low, gap_high)
            else:
                if float(c0.low) > float(c2.high):
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

        检测顺序（优先级从高到低）：
        1. FVG retest — 在 1m 上找 FVG（[BUG7 修复]）并检测回踩
        2. Engulfing  — 吞没烛
        3. Pinbar     — 长影线蜡烛（[BUG13 修复] 允许阴线 Hammer）

        [BUG14 修复] 同一根 K 线只触发一次信号。
        """
        if len(df_1m) < 3:
            return None

        direction = "long" if manip.bias == "bullish" else "short"
        last = df_1m.iloc[-1]
        prev = df_1m.iloc[-2]

        # [BUG14] 去重：同一根 K 线不重复触发
        last_ts = df_1m.index[-1]
        if isinstance(last_ts, pd.Timestamp):
            last_ts_dt = last_ts.to_pydatetime()
        else:
            last_ts_dt = last_ts
        if last_ts_dt == self._last_entry_candle_ts:
            return None

        # [BUG7] 在 1m 数据上寻找 FVG
        fvg = self.find_fvg_1m(df_1m, direction)
        if fvg:
            # 将 FVG 信息注入 manipulation（不修改原对象，创建副本）
            manip_with_fvg = ManipulationEvent(
                direction=manip.direction,
                extreme=manip.extreme,
                bias=manip.bias,
                acc_range=manip.acc_range,
                timestamp=manip.timestamp,
                fvg_low=fvg[0],
                fvg_high=fvg[1],
            )
        else:
            manip_with_fvg = manip

        # ── 优先级 1：FVG retest ──
        if manip_with_fvg.has_fvg:
            signal = self._check_fvg_retest(last, manip_with_fvg, direction)
            if signal:
                self._last_entry_candle_ts = last_ts_dt
                return signal

        # ── 优先级 2：Engulfing ──
        signal = self._check_engulfing(prev, last, direction, manip_with_fvg)
        if signal:
            self._last_entry_candle_ts = last_ts_dt
            return signal

        # ── 优先级 3：Pinbar ──
        signal = self._check_pinbar(last, direction, manip_with_fvg)
        if signal:
            self._last_entry_candle_ts = last_ts_dt
            return signal

        return None

    def reset_entry_dedup(self) -> None:
        """换新 Manipulation 时重置入场去重状态"""
        self._last_entry_candle_ts = None

    # ──────────────────── 信号检测子函数 ─────────────────────────

    def _check_fvg_retest(
        self, candle, manip: ManipulationEvent, direction: str
    ) -> Optional[EntrySignal]:
        """价格回踩 1m FVG 区域"""
        fvg_low = manip.fvg_low
        fvg_high = manip.fvg_high
        close = float(candle.close)
        low = float(candle.low)
        high = float(candle.high)

        if direction == "long":
            # 低点触及 FVG，收盘在 FVG 内或以上（反弹迹象）
            if low <= fvg_high and close >= fvg_low:
                entry = close
                sl = manip.extreme - manip.acc_range.atr * self.cfg.sl_atr_buffer
                logger.info(f"[SIGNAL] FVG retest LONG @ {entry:.2f}")
                return EntrySignal(
                    direction="long", entry_price=entry,
                    stop_loss=sl, manipulation=manip,
                    signal_type="fvg_retest",
                    candle_ts=candle.Index.to_pydatetime()
                    if hasattr(candle.Index, "to_pydatetime") else datetime.now(),
                )
        else:
            # 高点触及 FVG，收盘在 FVG 内或以下（回落迹象）
            if high >= fvg_low and close <= fvg_high:
                entry = close
                sl = manip.extreme + manip.acc_range.atr * self.cfg.sl_atr_buffer
                logger.info(f"[SIGNAL] FVG retest SHORT @ {entry:.2f}")
                return EntrySignal(
                    direction="short", entry_price=entry,
                    stop_loss=sl, manipulation=manip,
                    signal_type="fvg_retest",
                    candle_ts=candle.Index.to_pydatetime()
                    if hasattr(candle.Index, "to_pydatetime") else datetime.now(),
                )
        return None

    def _check_engulfing(
        self, prev, last, direction: str, manip: ManipulationEvent
    ) -> Optional[EntrySignal]:
        """吞没烛：当前阳/阴线实体完全吞噬前一根实体"""
        prev_body_top = max(float(prev.open), float(prev.close))
        prev_body_bot = min(float(prev.open), float(prev.close))
        last_body_top = max(float(last.open), float(last.close))
        last_body_bot = min(float(last.open), float(last.close))

        # 前一根实体必须有实质大小（避免十字星被吞没）
        prev_body = prev_body_top - prev_body_bot
        if prev_body < manip.acc_range.atr * 0.05:
            return None

        if direction == "long":
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
                    candle_ts=last.Index.to_pydatetime()
                    if hasattr(last.Index, "to_pydatetime") else datetime.now(),
                )
        else:
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
                    candle_ts=last.Index.to_pydatetime()
                    if hasattr(last.Index, "to_pydatetime") else datetime.now(),
                )
        return None

    def _check_pinbar(
        self, candle, direction: str, manip: ManipulationEvent
    ) -> Optional[EntrySignal]:
        """
        Pinbar：影线长度 >= 实体长度 * 2。

        [BUG13 修复] 不强制要求阳线/阴线，允许阴线 Hammer（红色锤子线）。
        条件只看影线比例，不限制收盘颜色。
        """
        open_ = float(candle.open)
        close = float(candle.close)
        high = float(candle.high)
        low = float(candle.low)

        body = abs(close - open_)
        upper_wick = high - max(open_, close)
        lower_wick = min(open_, close) - low

        # 实体不能是十字星（太小的实体 pinbar 不可靠）
        if body < manip.acc_range.atr * 0.03:
            return None

        if direction == "long":
            # 看涨 Pinbar：长下影线（>= 实体 * 2），上影线短（< 实体）
            if lower_wick >= body * 2 and upper_wick <= body:
                entry = close
                sl = manip.extreme - manip.acc_range.atr * self.cfg.sl_atr_buffer
                logger.info(f"[SIGNAL] Pinbar LONG @ {entry:.2f} (body={'阳' if close>open_ else '阴'}线)")
                return EntrySignal(
                    direction="long", entry_price=entry,
                    stop_loss=sl, manipulation=manip,
                    signal_type="pinbar",
                    candle_ts=candle.Index.to_pydatetime()
                    if hasattr(candle.Index, "to_pydatetime") else datetime.now(),
                )
        else:
            # 看跌 Pinbar：长上影线（>= 实体 * 2），下影线短（< 实体）
            if upper_wick >= body * 2 and lower_wick <= body:
                entry = close
                sl = manip.extreme + manip.acc_range.atr * self.cfg.sl_atr_buffer
                logger.info(f"[SIGNAL] Pinbar SHORT @ {entry:.2f} (body={'阳' if close>open_ else '阴'}线)")
                return EntrySignal(
                    direction="short", entry_price=entry,
                    stop_loss=sl, manipulation=manip,
                    signal_type="pinbar",
                    candle_ts=candle.Index.to_pydatetime()
                    if hasattr(candle.Index, "to_pydatetime") else datetime.now(),
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
        """获取最新 ATR 值，失败时返回价格的 0.2% 作为兜底"""
        if len(df) < length + 2:
            # 兜底：用最近收盘价的 0.2%
            if len(df) > 0:
                return float(df["close"].iloc[-1]) * 0.002
            return 0.0
        atr_series = ta.atr(df["high"], df["low"], df["close"], length=length)
        if atr_series is None or atr_series.isna().all():
            return float(df["close"].iloc[-1]) * 0.002
        val = float(atr_series.iloc[-1])
        return val if val > 0 else float(df["close"].iloc[-1]) * 0.002
