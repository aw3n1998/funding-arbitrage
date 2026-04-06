"""
风险管理模块

修复记录：
  - [BUG5]  calculate_position_size 增加杠杆上限约束
  - [BUG10] 权益获取失败时不再使用默认值，返回 None 拒绝开仓
"""
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional, Tuple

from loguru import logger


@dataclass
class TradeRecord:
    """单笔交易记录"""
    trade_id: str
    direction: str
    entry_price: float
    stop_loss: float
    tp1: float
    tp2: float
    contracts: float
    risk_usdt: float
    equity_at_entry: float
    opened_at: datetime = field(default_factory=datetime.now)
    closed_at: Optional[datetime] = None
    pnl: float = 0.0
    status: str = "open"    # "open" | "tp1" | "closed" | "sl"


class RiskManager:
    """
    复利风险管理器

    每次入场前：
    1. 检查每日交易次数和亏损上限
    2. 计算本次仓位大小（动态根据当前权益）
       - [BUG5] 强制不超过 leverage × equity 的名义价值上限
    3. 计算 TP1/TP2/SL 价位
    """

    def __init__(self, config):
        self.cfg = config
        self._today: date = date.today()
        self._daily_trades: int = 0
        self._daily_start_equity: float = 0.0
        self._trade_counter: int = 0

    # ──────────────── 每日重置 ────────────────

    def set_daily_start_equity(self, equity: float) -> None:
        """每日开始时记录起始权益（也用于日内重置判断）"""
        if self._daily_start_equity <= 0:
            self._daily_start_equity = equity
        self._reset_if_new_day(equity)

    def _reset_if_new_day(self, equity: float) -> None:
        today = date.today()
        if today != self._today:
            logger.info(
                f"[RISK] 新交易日 {today} | "
                f"昨日交易: {self._daily_trades} 次 | "
                f"起始权益: {equity:.2f} USDT"
            )
            self._today = today
            self._daily_trades = 0
            self._daily_start_equity = equity

    # ──────────────── 交易许可检查 ────────────────

    def can_trade(self, current_equity: Optional[float]) -> Tuple[bool, str]:
        """
        [BUG10 修复] current_equity 为 None 时直接拒绝（不使用默认值）。
        返回 (是否可以交易, 原因说明)
        """
        # 权益为 None = API 获取失败，拒绝开仓
        if current_equity is None:
            return False, "账户权益获取失败，拒绝开仓"

        self._reset_if_new_day(current_equity)

        if self._daily_start_equity <= 0:
            self._daily_start_equity = current_equity

        # 检查交易次数
        if self._daily_trades >= self.cfg.max_daily_trades:
            msg = (
                f"已达每日交易上限 {self.cfg.max_daily_trades} 次 "
                f"(今日: {self._daily_trades})"
            )
            logger.warning(f"[RISK] {msg}")
            return False, msg

        # 检查每日亏损
        daily_loss_pct = self.daily_loss_pct(current_equity)
        if daily_loss_pct >= self.cfg.max_daily_loss:
            msg = (
                f"已达每日亏损上限 "
                f"{daily_loss_pct*100:.1f}% >= {self.cfg.max_daily_loss*100:.1f}%"
            )
            logger.warning(f"[RISK] {msg}")
            return False, msg

        return True, ""

    # ──────────────── 仓位大小计算 ────────────────

    def calculate_position_size(
        self,
        equity: float,
        entry_price: float,
        stop_loss: float,
    ) -> float:
        """
        基于固定风险比例计算仓位（合约张数）。

        公式：
            风险金额   = equity × risk_per_trade
            SL 距离%  = |entry - SL| / entry
            仓位 USDT = 风险金额 / SL距离%

        [BUG5 修复] 仓位 USDT 不超过 equity × leverage（杠杆名义上限）。
        """
        sl_distance = abs(entry_price - stop_loss)
        if sl_distance <= 0:
            logger.error("[RISK] SL距离为0，拒绝开仓")
            return 0.0

        risk_usdt = equity * self.cfg.risk_per_trade
        sl_pct = sl_distance / entry_price
        position_usdt = risk_usdt / sl_pct

        # [BUG5] 杠杆名义上限
        max_position_usdt = equity * self.cfg.leverage
        if position_usdt > max_position_usdt:
            logger.warning(
                f"[RISK] 仓位 {position_usdt:.2f} USDT 超过 {self.cfg.leverage}x 上限 "
                f"{max_position_usdt:.2f} USDT，截断至上限"
            )
            position_usdt = max_position_usdt

        contracts = position_usdt / entry_price

        logger.info(
            f"[RISK] 仓位计算 | 权益:{equity:.2f} "
            f"风险:{risk_usdt:.2f}USDT ({self.cfg.risk_per_trade*100:.1f}%) | "
            f"SL:{sl_distance:.2f}({sl_pct*100:.3f}%) | "
            f"仓位:{position_usdt:.2f}USDT | 合约:{contracts:.4f}"
        )
        return round(contracts, 4)

    # ──────────────── TP/SL 计算 ────────────────

    def calculate_tp_sl(
        self,
        entry: float,
        manipulation_extreme: float,
        atr: float,
        direction: str,
    ) -> Tuple[float, float, float]:
        """
        返回 (sl, tp1, tp2)

        SL  = manipulation extreme 外侧 ATR × sl_atr_buffer
        TP1 = entry ± SL距离 × tp1_rr
        TP2 = entry ± SL距离 × tp2_rr
        """
        buffer = atr * self.cfg.sl_atr_buffer

        if direction == "long":
            sl = manipulation_extreme - buffer
            sl_dist = entry - sl
            if sl_dist <= 0:
                sl = entry * 0.995
                sl_dist = entry - sl
            tp1 = entry + sl_dist * self.cfg.tp1_rr
            tp2 = entry + sl_dist * self.cfg.tp2_rr
        else:
            sl = manipulation_extreme + buffer
            sl_dist = sl - entry
            if sl_dist <= 0:
                sl = entry * 1.005
                sl_dist = sl - entry
            tp1 = entry - sl_dist * self.cfg.tp1_rr
            tp2 = entry - sl_dist * self.cfg.tp2_rr

        logger.info(
            f"[RISK] TP/SL | {direction} 入场:{entry:.2f} "
            f"SL:{sl:.2f} TP1:{tp1:.2f}(RR{self.cfg.tp1_rr}) "
            f"TP2:{tp2:.2f}(RR{self.cfg.tp2_rr})"
        )
        return round(sl, 2), round(tp1, 2), round(tp2, 2)

    # ──────────────── 记账 ────────────────

    def record_trade_open(self, record: TradeRecord) -> None:
        self._daily_trades += 1
        self._trade_counter += 1
        logger.info(
            f"[RISK] 开仓记录 今日第{self._daily_trades}笔 | "
            f"剩余次数: {self.cfg.max_daily_trades - self._daily_trades}"
        )

    def record_trade_close(self, pnl: float) -> None:
        logger.info(f"[RISK] 平仓记录 PnL: {pnl:+.4f} USDT")

    @property
    def daily_trades_count(self) -> int:
        return self._daily_trades

    @property
    def daily_trades_remaining(self) -> int:
        return max(0, self.cfg.max_daily_trades - self._daily_trades)

    def daily_loss_pct(self, current_equity: float) -> float:
        if self._daily_start_equity <= 0:
            return 0.0
        return (self._daily_start_equity - current_equity) / self._daily_start_equity
