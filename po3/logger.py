"""
结构化交易日志

每笔交易事件写入两个地方：
1. 控制台（loguru 彩色日志）
2. data/trades_YYYYMMDD.json（JSON 追加，便于事后分析）
"""
import json
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from loguru import logger


class TradeLogger:
    """JSON 结构化交易日志记录器"""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)

    def _log_path(self) -> Path:
        return self.data_dir / f"trades_{date.today().strftime('%Y%m%d')}.json"

    def _write(self, event: dict) -> None:
        """追加写入 JSON 日志（每行一个 JSON 对象）"""
        try:
            with open(self._log_path(), "a", encoding="utf-8") as f:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"[TLOG] 写入日志失败: {e}")

    # ──────────────────── 事件记录 ────────────────────

    def log_entry(self, position, signal, equity: float, atr: float) -> None:
        """记录开仓事件"""
        manip = signal.manipulation
        acc = manip.acc_range
        event = {
            "timestamp": datetime.now().isoformat(),
            "event": "entry",
            "trade_id": position.trade_id,
            "direction": position.direction,
            "symbol": position.symbol,
            "entry_price": position.entry_price,
            "stop_loss": position.stop_loss,
            "tp1": position.tp1,
            "tp2": position.tp2,
            "contracts": position.contracts_total,
            "equity": round(equity, 4),
            "risk_usdt": round(equity * 0.01, 4),
            "atr": round(atr, 4),
            "signal_type": signal.signal_type,
            "po3_bias": manip.bias,
            "manip_direction": manip.direction,
            "manip_extreme": manip.extreme,
            "acc_high": acc.high,
            "acc_low": acc.low,
            "fvg_high": manip.fvg_high,
            "fvg_low": manip.fvg_low,
        }
        self._write(event)
        logger.info(
            f"[TLOG] ENTRY {position.direction.upper()} | "
            f"入场:{position.entry_price:.2f} SL:{position.stop_loss:.2f} "
            f"TP1:{position.tp1:.2f} TP2:{position.tp2:.2f} | "
            f"信号:{signal.signal_type} 合约:{position.contracts_total}"
        )

    def log_tp1(self, position) -> None:
        """记录 TP1 成交事件"""
        # [H5] 从持仓字段推算实际平仓量，不硬编码 0.5
        # _on_tp1_filled 调用此方法前已更新 contracts_remaining
        contracts_closed = round(
            position.contracts_total - position.contracts_remaining, 4
        )
        close_pct = round(contracts_closed / position.contracts_total * 100, 1) if position.contracts_total > 0 else 50.0
        event = {
            "timestamp": datetime.now().isoformat(),
            "event": "tp1_filled",
            "trade_id": position.trade_id,
            "direction": position.direction,
            "tp1_price": position.tp1,
            "contracts_closed": contracts_closed,
            "contracts_remaining": position.contracts_remaining,
            "tp1_pnl": round(position.tp1_pnl, 4),
        }
        self._write(event)
        logger.info(
            f"[TLOG] TP1 成交 @ {position.tp1:.2f} | "
            f"平仓{close_pct:.0f}% ({contracts_closed:.4f}张) "
            f"剩余:{position.contracts_remaining:.4f}张 "
            f"TP1 PnL:{position.tp1_pnl:+.4f} USDT"
        )

    def log_close(
        self,
        position,
        close_price: float,
        pnl: float,
        reason: str,
    ) -> None:
        """记录平仓事件"""
        holding_secs = (
            datetime.now() - position.opened_at
        ).total_seconds()
        event = {
            "timestamp": datetime.now().isoformat(),
            "event": "close",
            "trade_id": position.trade_id,
            "direction": position.direction,
            "entry_price": position.entry_price,
            "close_price": close_price,
            "pnl_usdt": round(pnl, 4),
            "reason": reason,
            "holding_seconds": int(holding_secs),
            "contracts_total": position.contracts_total,
        }
        self._write(event)
        pnl_str = f"{pnl:+.4f}"
        logger.info(
            f"[TLOG] CLOSE {position.direction.upper()} | "
            f"原因:{reason} 平价:{close_price:.2f} "
            f"PnL:{pnl_str} USDT | 持仓:{holding_secs:.0f}s"
        )

    def log_po3_phase(
        self,
        phase: str,
        detail: str,
        symbol: str,
    ) -> None:
        """记录 PO3 阶段识别事件"""
        event = {
            "timestamp": datetime.now().isoformat(),
            "event": "po3_phase",
            "symbol": symbol,
            "phase": phase,
            "detail": detail,
        }
        self._write(event)
        logger.debug(f"[TLOG] PO3 phase={phase} | {detail}")

    def log_signal_rejected(self, reason: str, symbol: str) -> None:
        """记录信号被风控拒绝"""
        event = {
            "timestamp": datetime.now().isoformat(),
            "event": "signal_rejected",
            "symbol": symbol,
            "reason": reason,
        }
        self._write(event)
        logger.info(f"[TLOG] 信号拒绝: {reason}")

    def log_daily_summary(
        self,
        daily_trades: int,
        equity: float,
        daily_pnl_pct: float,
    ) -> None:
        """记录每日汇总"""
        event = {
            "timestamp": datetime.now().isoformat(),
            "event": "daily_summary",
            "date": date.today().isoformat(),
            "daily_trades": daily_trades,
            "equity": round(equity, 4),
            "daily_pnl_pct": round(daily_pnl_pct * 100, 2),
        }
        self._write(event)
        logger.info(
            f"[TLOG] 日度汇总 | 交易:{daily_trades}次 "
            f"权益:{equity:.2f} 日收益:{daily_pnl_pct*100:+.2f}%"
        )
