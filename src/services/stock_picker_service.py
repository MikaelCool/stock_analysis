# -*- coding: utf-8 -*-
"""A-share stock picking service."""

from __future__ import annotations

import json
import logging
import math
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
from sqlalchemy import and_, desc, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from data_provider import DataFetcherManager
from data_provider.baostock_fetcher import BaostockFetcher
from data_provider.tushare_fetcher import TushareFetcher
from src.agent.skills.base import load_skills_from_directory
from src.analyzer import GeminiAnalyzer
from src.config import get_config
from src.notification import NotificationService
from src.search_service import SearchResponse, SearchResult, SearchService
from src.storage import (
    DatabaseManager,
    NewsIntel,
    StockDaily,
    StockSelectionBacktest,
    StockSelectionCandidate,
    StockSelectionOptimization,
    StockSelectionRun,
)

logger = logging.getLogger(__name__)

BACKTEST_HORIZONS = (1, 3, 5, 10)
_BUILTIN_STRATEGY_DIR = Path(__file__).resolve().parents[2] / "strategies"
_MAX_SELECTED_CANDIDATES = 10
_MAX_REPORT_CANDIDATES = 5

_PICKER_STRATEGY_PRESETS: Dict[str, Dict[str, Any]] = {
    "mainboard_swing_master": {
        "scan_family": "mainboard_swing_master",
        "priority": 5,
        "params": {
            "min_score_threshold": 72.0,
            "volume_spike_multiplier": 1.6,
            "max_ma20_distance_pct": 4.0,
        },
    },
    "bull_trend": {
        "scan_family": "shrink_pullback",
        "priority": 10,
        "params": {
            "min_score_threshold": 70.0,
            "volume_spike_multiplier": 1.4,
            "max_ma20_distance_pct": 4.0,
        },
    },
    "ma_golden_cross": {
        "scan_family": "ma_golden_cross",
        "priority": 20,
        "params": {
            "min_score_threshold": 70.0,
            "volume_spike_multiplier": 1.6,
            "max_ma20_distance_pct": 3.5,
        },
    },
    "shrink_pullback": {
        "scan_family": "shrink_pullback",
        "priority": 30,
        "params": {
            "min_score_threshold": 68.0,
            "volume_spike_multiplier": 1.5,
            "max_ma20_distance_pct": 4.0,
        },
    },
    "volume_breakout": {
        "scan_family": "volume_breakout",
        "priority": 40,
        "params": {
            "min_score_threshold": 72.0,
            "volume_spike_multiplier": 2.0,
            "max_ma20_distance_pct": 5.0,
        },
    },
    "dragon_head": {
        "scan_family": "volume_breakout",
        "priority": 50,
        "params": {
            "min_score_threshold": 78.0,
            "volume_spike_multiplier": 2.2,
            "max_ma20_distance_pct": 5.5,
        },
    },
    "chan_theory": {
        "scan_family": "mainboard_swing_master",
        "priority": 60,
        "params": {
            "min_score_threshold": 72.0,
            "volume_spike_multiplier": 1.6,
            "max_ma20_distance_pct": 3.2,
        },
    },
    "wave_theory": {
        "scan_family": "mainboard_swing_master",
        "priority": 70,
        "params": {
            "min_score_threshold": 71.0,
            "volume_spike_multiplier": 1.6,
            "max_ma20_distance_pct": 3.8,
        },
    },
    "emotion_cycle": {
        "scan_family": "mainboard_swing_master",
        "priority": 80,
        "params": {
            "min_score_threshold": 66.0,
            "volume_spike_multiplier": 1.3,
            "max_ma20_distance_pct": 4.5,
        },
    },
    "box_oscillation": {
        "scan_family": "volume_breakout",
        "priority": 90,
        "params": {
            "min_score_threshold": 67.0,
            "volume_spike_multiplier": 1.5,
            "max_ma20_distance_pct": 4.5,
        },
    },
    "bottom_volume": {
        "scan_family": "volume_breakout",
        "priority": 100,
        "params": {
            "min_score_threshold": 65.0,
            "volume_spike_multiplier": 1.4,
            "max_ma20_distance_pct": 4.8,
        },
    },
    "one_yang_three_yin": {
        "scan_family": "shrink_pullback",
        "priority": 110,
        "params": {
            "min_score_threshold": 69.0,
            "volume_spike_multiplier": 1.4,
            "max_ma20_distance_pct": 4.0,
        },
    },
}


@dataclass(frozen=True)
class StrategyDefinition:
    strategy_id: str
    name: str
    description: str
    skill_id: str
    category: str
    params: Dict[str, Any]


@dataclass
class StrategySignal:
    passed: bool
    score: float
    setup_type: str
    operation_advice: str
    analysis_summary: str
    reasons: List[str]
    stop_loss: Optional[float]
    take_profit: Optional[float]
    metrics: Dict[str, Any]


class StockPickerService:
    """Quantitative scan + LLM review workflow for post-close stock picking."""

    _background_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="picker_bg")
    _schedule_lock = threading.Lock()

    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        *,
        config=None,
    ) -> None:
        self.config = config or get_config()
        self.db = db_manager or DatabaseManager.get_instance()
        self.fetcher_manager = DataFetcherManager()
        self.tushare_fetcher = TushareFetcher(
            rate_limit_per_minute=getattr(self.config, "tushare_rate_limit_per_minute", 80)
        )
        self.search_service = None
        try:
            self.search_service = SearchService(
                bocha_keys=self.config.bocha_api_keys,
                tavily_keys=self.config.tavily_api_keys,
                anspire_keys=self.config.anspire_api_keys,
                brave_keys=self.config.brave_api_keys,
                serpapi_keys=self.config.serpapi_keys,
                minimax_keys=self.config.minimax_api_keys,
                searxng_base_urls=self.config.searxng_base_urls,
                searxng_public_instances_enabled=self.config.searxng_public_instances_enabled,
                news_max_age_days=self.config.news_max_age_days,
                news_strategy_profile=getattr(self.config, "news_strategy_profile", "short"),
            )
        except Exception as exc:
            logger.warning("Stock picker search service unavailable: %s", exc)
        self.analyzer = GeminiAnalyzer(config=self.config)

    @staticmethod
    def list_strategies() -> List[Dict[str, Any]]:
        strategies = [
            StrategyDefinition(
                strategy_id="mainboard_swing_master",
                name="主力波段双模",
                description="收盘后筛选主板非 ST 标的，兼顾突破启动和趋势回踩，叠加市场情绪与消息面审核。",
                skill_id="swing_after_close_picker",
                category="swing",
                params={
                    "min_score_threshold": 75.0,
                    "volume_spike_multiplier": 1.8,
                    "max_ma20_distance_pct": 3.0,
                },
            ),
            StrategyDefinition(
                strategy_id="ma_golden_cross",
                name="均线金叉",
                description="聚焦 5 日线上穿 20 日线、60 日线向上的启动段。",
                skill_id="ma_golden_cross",
                category="trend",
                params={
                    "min_score_threshold": 70.0,
                    "volume_spike_multiplier": 1.6,
                    "max_ma20_distance_pct": 3.5,
                },
            ),
            StrategyDefinition(
                strategy_id="shrink_pullback",
                name="缩量回踩",
                description="聚焦 MA5/10/20/30 多头排列中的缩量回踩与再启动。",
                skill_id="shrink_pullback",
                category="trend",
                params={
                    "min_score_threshold": 68.0,
                    "volume_spike_multiplier": 1.5,
                    "max_ma20_distance_pct": 4.0,
                },
            ),
            StrategyDefinition(
                strategy_id="volume_breakout",
                name="放量突破",
                description="聚焦近端平台突破、倍量启动与板块联动的强势票。",
                skill_id="volume_breakout",
                category="breakout",
                params={
                    "min_score_threshold": 72.0,
                    "volume_spike_multiplier": 2.0,
                    "max_ma20_distance_pct": 5.0,
                },
            ),
        ]
        return [
            {
                "strategy_id": item.strategy_id,
                "name": item.name,
                "description": item.description,
                "skill_id": item.skill_id,
                "category": item.category,
                "params": dict(item.params),
            }
            for item in strategies
        ]

    def run_scan(
        self,
        *,
        strategy_id: Optional[str] = None,
        scan_date: Optional[date] = None,
        max_candidates: Optional[int] = None,
        strategy_params: Optional[Dict[str, Any]] = None,
        send_notification: bool = False,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        if not getattr(self.config, "stock_picker_enabled", True):
            raise ValueError("stock picker is disabled")

        strategy = self._resolve_strategy(strategy_id)
        optimized = self._get_latest_optimization(strategy["strategy_id"])
        strategy_params_override = self._normalize_strategy_params(strategy_params)
        strategy_params = dict(strategy["params"])
        if optimized and isinstance(optimized.get("params"), dict):
            strategy_params.update(optimized["params"])
        if strategy_params_override:
            strategy_params.update(strategy_params_override)

        effective_scan_date = scan_date or self._resolve_scan_trade_date()
        max_selected = min(
            max_candidates or getattr(self.config, "stock_picker_max_candidates", _MAX_SELECTED_CANDIDATES),
            _MAX_SELECTED_CANDIDATES,
        )
        llm_review_limit = min(
            getattr(self.config, "stock_picker_llm_review_limit", _MAX_REPORT_CANDIDATES),
            _MAX_REPORT_CANDIDATES,
            max_selected,
        )

        self._ensure_recent_market_data(effective_scan_date, lookback_trading_days=90, force_refresh=force_refresh)
        universe = self._load_mainboard_universe()
        market_snapshot = self._build_market_snapshot()
        us_snapshot = self._build_us_market_snapshot()
        history_frames = self._load_history_frames([item["code"] for item in universe], effective_scan_date)

        candidates: List[Dict[str, Any]] = []
        total_scanned = 0
        for item in universe:
            code = item["code"]
            df = history_frames.get(code)
            if df is None or df.empty:
                continue
            total_scanned += 1
            signal = self._evaluate_strategy(
                strategy["strategy_id"],
                code=code,
                name=item["name"],
                bars=df,
                market_snapshot=market_snapshot,
                params=strategy_params,
            )
            if not signal.passed:
                continue
            candidates.append(
                {
                    "code": code,
                    "name": item["name"],
                    "scan_date": effective_scan_date.isoformat(),
                    "strategy_id": strategy["strategy_id"],
                    "setup_type": signal.setup_type,
                    "score": round(signal.score, 2),
                    "operation_advice": signal.operation_advice,
                    "analysis_summary": signal.analysis_summary,
                    "reasons": signal.reasons,
                    "stop_loss": signal.stop_loss,
                    "take_profit": signal.take_profit,
                    "metrics": signal.metrics,
                    "news_context": None,
                    "market_context": {
                        "cn": market_snapshot,
                        "us": us_snapshot,
                    },
                    "action_plan_markdown": "",
                    "llm_model": None,
                }
            )

        candidates.sort(key=lambda item: item["score"], reverse=True)
        matched_count = len(candidates)
        selected = candidates[:max_selected]

        for rank, candidate in enumerate(selected, start=1):
            candidate["rank"] = rank
        for idx, candidate in enumerate(selected):
            candidate["score"] = round(
                candidate["score"] * 0.86 + float(market_snapshot.get("score", 50.0)) * 0.14,
                2,
            )
            if idx < llm_review_limit:
                candidate["action_plan_markdown"] = "后台正在生成次日操作手册和消息面复核，完成后会自动更新。"
            else:
                candidate["action_plan_markdown"] = "该票保留为量化候选，系统只对前 5 只生成详细操作手册。"
            candidate["llm_model"] = None
            if idx < llm_review_limit:
                candidate["action_plan_markdown"] = self._build_template_action_plan(
                    candidate=candidate,
                    strategy=strategy,
                    market_snapshot=market_snapshot,
                    us_snapshot=us_snapshot,
                    review_text=None,
                )
            else:
                candidate["action_plan_markdown"] = "该票保留为量化候选，系统只对前 5 只生成详细操作手册。"

        query_id = uuid.uuid4().hex
        run_id = self._save_scan_run(
            query_id=query_id,
            strategy=strategy,
            scan_date=effective_scan_date,
            total_scanned=total_scanned,
            matched_count=matched_count,
            selected=selected,
            market_snapshot=market_snapshot,
            us_snapshot=us_snapshot,
            optimization=optimized,
            status="queued",
        )
        self._enqueue_run_enrichment(
            run_id=run_id,
            query_id=query_id,
            strategy=strategy,
            scan_date=effective_scan_date,
            llm_review_limit=llm_review_limit,
            market_snapshot=market_snapshot,
            us_snapshot=us_snapshot,
            send_notification=send_notification,
        )

        result = self.get_run_detail(run_id)
        if result is None:
            raise RuntimeError("scan run saved but detail lookup failed")
        if send_notification:
            result["notification_stage"] = "initial"
            self._send_scan_notification(result)
        return result

    @staticmethod
    def _normalize_strategy_params(raw_params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {}
        if not isinstance(raw_params, dict):
            return normalized
        for key, value in raw_params.items():
            if value in (None, ""):
                continue
            if isinstance(value, (int, float)):
                normalized[str(key)] = float(value)
                continue
            try:
                normalized[str(key)] = float(str(value).strip())
            except (TypeError, ValueError):
                normalized[str(key)] = value
        return normalized

    def run_scheduled_scan(self) -> Dict[str, Any]:
        strategy_id = "mainboard_swing_master"
        scan_date = self._resolve_scan_trade_date()

        with self._schedule_lock:
            existing_run = self._find_latest_run_for_date(strategy_id=strategy_id, scan_date=scan_date)
            if existing_run is not None:
                self._repair_stuck_run(run_id=existing_run)
                payload = self.get_run_detail(existing_run)
                if payload is not None:
                    return payload

            return self.run_scan(
                strategy_id=strategy_id,
                scan_date=scan_date,
                max_candidates=_MAX_SELECTED_CANDIDATES,
                send_notification=True,
            )

    def list_runs(self, limit: int = 20) -> List[Dict[str, Any]]:
        self._repair_stale_runs()
        with self.db.get_session() as session:
            rows = session.execute(
                select(StockSelectionRun)
                .order_by(desc(StockSelectionRun.scan_date), desc(StockSelectionRun.created_at))
                .limit(limit)
            ).scalars().all()
            return [self._run_to_dict(row, with_candidates=False) for row in rows]

    def get_run_detail(self, run_id: int) -> Optional[Dict[str, Any]]:
        self._repair_stale_runs()
        with self.db.get_session() as session:
            run = session.execute(
                select(StockSelectionRun).where(StockSelectionRun.id == run_id)
            ).scalar_one_or_none()
            if run is None:
                return None
            candidates = session.execute(
                select(StockSelectionCandidate)
                .where(StockSelectionCandidate.run_id == run.id)
                .order_by(StockSelectionCandidate.rank.asc(), StockSelectionCandidate.score.desc())
            ).scalars().all()
            repaired = False
            if candidates:
                strategy = self._resolve_strategy(run.strategy_id)
                market_snapshot = self._safe_json_loads(run.market_snapshot_json) or {}
                us_snapshot = self._safe_json_loads(run.us_market_snapshot_json) or {}
                for idx, item in enumerate(candidates):
                    if idx >= _MAX_REPORT_CANDIDATES:
                        break
                    if not self._is_placeholder_action_plan(item.action_plan_markdown):
                        continue
                    candidate_payload = {
                        "code": item.code,
                        "name": item.name,
                        "scan_date": item.scan_date.isoformat() if item.scan_date else None,
                        "strategy_id": item.strategy_id,
                        "setup_type": item.setup_type,
                        "score": float(item.score or 0.0),
                        "analysis_summary": item.analysis_summary,
                        "stop_loss": item.stop_loss,
                        "take_profit": item.take_profit,
                        "metrics": self._safe_json_loads(item.indicator_snapshot_json) or {},
                    }
                    item.action_plan_markdown = self._build_template_action_plan(
                        candidate=candidate_payload,
                        strategy=strategy,
                        market_snapshot=market_snapshot,
                        us_snapshot=us_snapshot,
                        review_text=None,
                    )
                    repaired = True
                if repaired:
                    session.commit()
            candidate_ids = [item.id for item in candidates]
            backtests_by_candidate: Dict[int, List[StockSelectionBacktest]] = {}
            if candidate_ids:
                rows = session.execute(
                    select(StockSelectionBacktest)
                    .where(StockSelectionBacktest.candidate_id.in_(candidate_ids))
                    .order_by(StockSelectionBacktest.horizon_days.asc())
                ).scalars().all()
                for row in rows:
                    backtests_by_candidate.setdefault(row.candidate_id, []).append(row)

            payload = self._run_to_dict(run, with_candidates=False)
            payload["candidates"] = [
                self._candidate_to_dict(item, backtests_by_candidate.get(item.id, []))
                for item in candidates
            ]
            return payload

    def _enqueue_run_enrichment(
        self,
        *,
        run_id: int,
        query_id: str,
        strategy: Dict[str, Any],
        scan_date: date,
        llm_review_limit: int,
        market_snapshot: Dict[str, Any],
        us_snapshot: Dict[str, Any],
        send_notification: bool,
    ) -> None:
        self._background_executor.submit(
            self._run_enrichment_worker,
            run_id,
            query_id,
            dict(strategy),
            scan_date,
            llm_review_limit,
            dict(market_snapshot),
            dict(us_snapshot),
            send_notification,
        )

    @classmethod
    def _run_enrichment_worker(
        cls,
        run_id: int,
        query_id: str,
        strategy: Dict[str, Any],
        scan_date: date,
        llm_review_limit: int,
        market_snapshot: Dict[str, Any],
        us_snapshot: Dict[str, Any],
        send_notification: bool,
    ) -> None:
        service = cls(config=get_config())
        service._update_run_status(run_id=run_id, status="enriching", completed=False)
        try:
            with service.db.session_scope() as session:
                candidate_rows = session.execute(
                    select(StockSelectionCandidate)
                    .where(StockSelectionCandidate.run_id == run_id)
                    .order_by(StockSelectionCandidate.rank.asc(), StockSelectionCandidate.score.desc())
                ).scalars().all()

                for idx, row in enumerate(candidate_rows):
                    if idx >= llm_review_limit:
                        row.action_plan_markdown = row.action_plan_markdown or "该票保留为量化候选，系统只对前 5 只生成详细操作手册。"
                        continue

                    candidate_payload = {
                        "code": row.code,
                        "name": row.name,
                        "strategy_id": row.strategy_id,
                        "setup_type": row.setup_type,
                        "score": float(row.score or 0.0),
                        "operation_advice": row.operation_advice,
                        "analysis_summary": row.analysis_summary,
                        "reasons": service._safe_json_loads(row.reason_json) or [],
                        "stop_loss": row.stop_loss,
                        "take_profit": row.take_profit,
                        "metrics": service._safe_json_loads(row.indicator_snapshot_json) or {},
                        "market_context": service._safe_json_loads(row.market_context_json) or {},
                    }
                    review_payload = service._collect_candidate_intel(
                        code=row.code,
                        name=row.name or row.code,
                        scan_date=scan_date,
                        query_id=query_id,
                    )
                    row.news_context_json = json.dumps(review_payload.get("news_context"), ensure_ascii=False)
                    action_plan, llm_model = service._build_action_plan(
                        candidate=candidate_payload,
                        strategy=strategy,
                        market_snapshot=market_snapshot,
                        us_snapshot=us_snapshot,
                        review_payload=review_payload,
                    )
                    row.action_plan_markdown = action_plan
                    row.llm_model = llm_model

            service._update_run_status(run_id=run_id, status="completed", completed=True)
            payload = service.get_run_detail(run_id)
            if payload is not None:
                if send_notification:
                    payload["notification_stage"] = "enhanced"
                    service._send_scan_notification(payload)

            backtest_stats = service.backfill_backtests(strategy_id=strategy["strategy_id"])
            optimization = service.optimize_strategy(
                strategy_id=strategy["strategy_id"],
                selected_horizon_days=5,
            )
            service._update_run_optimization(run_id=run_id, optimization=optimization)

            payload = service.get_run_detail(run_id)
            if payload is not None:
                payload["backtest_stats"] = backtest_stats
                payload["optimization"] = optimization
        except Exception as exc:
            logger.exception("Stock picker background enrichment failed for run %s: %s", run_id, exc)
            current = service.get_run_detail(run_id)
            if current is None or current.get("status") != "completed":
                service._update_run_status(run_id=run_id, status="failed", completed=False)

    @staticmethod
    def _is_placeholder_action_plan(value: Optional[str]) -> bool:
        text = (value or "").strip()
        if not text:
            return True
        if "后台正在生成次日操作手册" in text:
            return True
        malformed_markers = (
            "收盘 -",
            "MA5/10/20/30/60=-/-/-/-/-",
            "低吸挂单价：0.01",
            "突破跟单价：0.01",
            "止损价：0.01",
        )
        return any(marker in text for marker in malformed_markers)

    def _repair_stuck_run(self, *, run_id: int) -> None:
        with self.db.session_scope() as session:
            run = session.execute(
                select(StockSelectionRun).where(StockSelectionRun.id == run_id)
            ).scalar_one_or_none()
            if run is None or run.status not in ("queued", "enriching"):
                return

            strategy = self._resolve_strategy(run.strategy_id)
            market_snapshot = self._safe_json_loads(run.market_snapshot_json) or {}
            us_snapshot = self._safe_json_loads(run.us_market_snapshot_json) or {}
            candidate_rows = session.execute(
                select(StockSelectionCandidate)
                .where(StockSelectionCandidate.run_id == run_id)
                .order_by(StockSelectionCandidate.rank.asc(), StockSelectionCandidate.score.desc())
            ).scalars().all()

            repaired = False
            for idx, row in enumerate(candidate_rows):
                if idx >= _MAX_REPORT_CANDIDATES:
                    break
                if not self._is_placeholder_action_plan(row.action_plan_markdown):
                    continue
                candidate_payload = {
                    "code": row.code,
                    "name": row.name,
                    "scan_date": row.scan_date.isoformat() if row.scan_date else None,
                    "strategy_id": row.strategy_id,
                    "setup_type": row.setup_type,
                    "score": float(row.score or 0.0),
                    "analysis_summary": row.analysis_summary,
                    "stop_loss": row.stop_loss,
                    "take_profit": row.take_profit,
                    "metrics": self._safe_json_loads(row.metrics_json) or {},
                }
                row.action_plan_markdown = self._build_template_action_plan(
                    candidate=candidate_payload,
                    strategy=strategy,
                    market_snapshot=market_snapshot,
                    us_snapshot=us_snapshot,
                    review_text=None,
                )
                repaired = True

            if not repaired:
                return

            run.status = "completed"
            run.completed_at = datetime.now()

        payload = self.get_run_detail(run_id)
        if payload is not None:
            payload["notification_stage"] = "enhanced"
            self._send_scan_notification(payload)

    def _repair_stale_runs(self, *, older_than_minutes: int = 10) -> None:
        cutoff = datetime.now() - timedelta(minutes=max(1, older_than_minutes))
        with self.db.get_session() as session:
            run_ids = session.execute(
                select(StockSelectionRun.id)
                .where(
                    and_(
                        StockSelectionRun.status.in_(("queued", "enriching")),
                        StockSelectionRun.created_at <= cutoff,
                    )
                )
                .order_by(StockSelectionRun.created_at.asc())
                .limit(5)
            ).scalars().all()
        for run_id in run_ids:
            try:
                self._repair_stuck_run(run_id=int(run_id))
            except Exception as exc:
                logger.warning("Failed to repair stale stock picker run %s: %s", run_id, exc)

    def _find_latest_run_for_date(self, *, strategy_id: str, scan_date: date) -> Optional[int]:
        with self.db.get_session() as session:
            row = session.execute(
                select(StockSelectionRun.id)
                .where(
                    and_(
                        StockSelectionRun.strategy_id == strategy_id,
                        StockSelectionRun.scan_date == scan_date,
                        StockSelectionRun.status.in_(("queued", "enriching", "completed")),
                    )
                )
                .order_by(desc(StockSelectionRun.created_at))
                .limit(1)
            ).scalars().first()
        return int(row) if row is not None else None

    def _update_run_status(self, *, run_id: int, status: str, completed: bool) -> None:
        with self.db.session_scope() as session:
            run = session.execute(
                select(StockSelectionRun).where(StockSelectionRun.id == run_id)
            ).scalar_one_or_none()
            if run is None:
                return
            run.status = status
            if completed:
                run.completed_at = datetime.now()

    def backfill_backtests(
        self,
        *,
        strategy_id: Optional[str] = None,
        max_candidates: int = 200,
    ) -> Dict[str, Any]:
        processed = 0
        completed = 0
        pending = 0

        with self.db.get_session() as session:
            conditions = []
            if strategy_id:
                conditions.append(StockSelectionCandidate.strategy_id == strategy_id)

            existing_pairs = session.execute(
                select(
                    StockSelectionBacktest.candidate_id,
                    StockSelectionBacktest.horizon_days,
                )
            ).all()
            seen = {(candidate_id, horizon_days) for candidate_id, horizon_days in existing_pairs}

            query = select(StockSelectionCandidate)
            if conditions:
                query = query.where(and_(*conditions))
            query = query.order_by(desc(StockSelectionCandidate.scan_date), StockSelectionCandidate.rank.asc()).limit(max_candidates)
            candidates = session.execute(query).scalars().all()

            to_insert: List[StockSelectionBacktest] = []
            for candidate in candidates:
                bars = self._load_forward_bars(candidate.code, candidate.scan_date, max(BACKTEST_HORIZONS) + 1)
                if len(bars) < 1:
                    for horizon in BACKTEST_HORIZONS:
                        if (candidate.id, horizon) in seen:
                            continue
                        pending += 1
                        to_insert.append(
                            StockSelectionBacktest(
                                candidate_id=candidate.id,
                                strategy_id=candidate.strategy_id,
                                code=candidate.code,
                                scan_date=candidate.scan_date,
                                horizon_days=horizon,
                                status="pending",
                                created_at=datetime.now(),
                                evaluated_at=datetime.now(),
                            )
                        )
                    continue

                for horizon in BACKTEST_HORIZONS:
                    if (candidate.id, horizon) in seen:
                        continue
                    processed += 1
                    record = self._evaluate_candidate_horizon(candidate, bars, horizon)
                    if record["status"] == "completed":
                        completed += 1
                    else:
                        pending += 1
                    to_insert.append(
                        StockSelectionBacktest(
                            candidate_id=candidate.id,
                            strategy_id=candidate.strategy_id,
                            code=candidate.code,
                            scan_date=candidate.scan_date,
                            horizon_days=horizon,
                            status=record["status"],
                            entry_date=record.get("entry_date"),
                            exit_date=record.get("exit_date"),
                            entry_price=record.get("entry_price"),
                            exit_price=record.get("exit_price"),
                            end_close=record.get("end_close"),
                            max_high=record.get("max_high"),
                            min_low=record.get("min_low"),
                            return_pct=record.get("return_pct"),
                            max_drawdown_pct=record.get("max_drawdown_pct"),
                            outcome=record.get("outcome"),
                            created_at=datetime.now(),
                            evaluated_at=datetime.now(),
                        )
                    )

            if to_insert:
                session.add_all(to_insert)
                session.commit()

        return {
            "processed": processed,
            "completed": completed,
            "pending": pending,
        }

    def optimize_strategy(
        self,
        *,
        strategy_id: str,
        lookback_days: int = 90,
        selected_horizon_days: int = 5,
    ) -> Dict[str, Any]:
        strategy = self._resolve_strategy(strategy_id)
        cutoff = date.today() - timedelta(days=lookback_days)
        candidate_rows: List[Dict[str, Any]] = []
        with self.db.get_session() as session:
            rows = session.execute(
                select(StockSelectionCandidate, StockSelectionBacktest)
                .join(
                    StockSelectionBacktest,
                    StockSelectionBacktest.candidate_id == StockSelectionCandidate.id,
                )
                .where(
                    and_(
                        StockSelectionCandidate.strategy_id == strategy_id,
                        StockSelectionCandidate.scan_date >= cutoff,
                        StockSelectionBacktest.horizon_days == selected_horizon_days,
                        StockSelectionBacktest.status == "completed",
                    )
                )
            ).all()

            for candidate, backtest in rows:
                metrics = self._safe_json_loads(candidate.indicator_snapshot_json)
                if not isinstance(metrics, dict):
                    metrics = {}
                candidate_rows.append(
                    {
                        "score": float(candidate.score or 0.0),
                        "distance_to_ma20_pct": float(metrics.get("distance_to_ma20_pct") or 999.0),
                        "return_pct": float(backtest.return_pct or 0.0),
                    }
                )

            if not candidate_rows:
                payload = {
                    "strategy_id": strategy_id,
                    "strategy_name": strategy["name"],
                    "lookback_days": lookback_days,
                    "selected_horizon_days": selected_horizon_days,
                    "status": "insufficient_data",
                    "params": dict(strategy["params"]),
                    "metrics": {"sample_count": 0},
                }
                session.add(
                    StockSelectionOptimization(
                        strategy_id=strategy_id,
                        strategy_name=strategy["name"],
                        lookback_days=lookback_days,
                        selected_horizon_days=selected_horizon_days,
                        metrics_json=json.dumps(payload["metrics"], ensure_ascii=False),
                        params_json=json.dumps(payload["params"], ensure_ascii=False),
                        status="insufficient_data",
                        created_at=datetime.now(),
                    )
                )
                session.commit()
                return payload

            best_score = -10**9
            best_payload = None
            for min_score_threshold in (65.0, 70.0, 75.0, 80.0):
                for max_ma20_distance_pct in (1.5, 2.5, 3.5, 4.5):
                    filtered = [
                        row for row in candidate_rows
                        if row["score"] >= min_score_threshold
                        and row["distance_to_ma20_pct"] <= max_ma20_distance_pct
                    ]
                    if len(filtered) < 6:
                        continue
                    avg_return = sum(row["return_pct"] for row in filtered) / len(filtered)
                    win_rate = sum(1 for row in filtered if row["return_pct"] > 0) / len(filtered)
                    objective = avg_return * 0.75 + win_rate * 15.0
                    if objective > best_score:
                        best_score = objective
                        best_payload = {
                            "strategy_id": strategy_id,
                            "strategy_name": strategy["name"],
                            "lookback_days": lookback_days,
                            "selected_horizon_days": selected_horizon_days,
                            "status": "completed",
                            "params": {
                                "min_score_threshold": min_score_threshold,
                                "max_ma20_distance_pct": max_ma20_distance_pct,
                                "volume_spike_multiplier": strategy["params"]["volume_spike_multiplier"],
                            },
                            "metrics": {
                                "sample_count": len(filtered),
                                "avg_return_pct": round(avg_return, 2),
                                "win_rate_pct": round(win_rate * 100, 2),
                                "objective": round(objective, 4),
                            },
                        }

            if best_payload is None:
                best_payload = {
                    "strategy_id": strategy_id,
                    "strategy_name": strategy["name"],
                    "lookback_days": lookback_days,
                    "selected_horizon_days": selected_horizon_days,
                    "status": "insufficient_data",
                    "params": dict(strategy["params"]),
                    "metrics": {"sample_count": len(candidate_rows)},
                }

            session.add(
                StockSelectionOptimization(
                    strategy_id=strategy_id,
                    strategy_name=strategy["name"],
                    lookback_days=lookback_days,
                    selected_horizon_days=selected_horizon_days,
                    metrics_json=json.dumps(best_payload["metrics"], ensure_ascii=False),
                    params_json=json.dumps(best_payload["params"], ensure_ascii=False),
                    status=best_payload["status"],
                    created_at=datetime.now(),
                )
            )
            session.commit()

        return best_payload

    def _resolve_strategy(self, strategy_id: Optional[str]) -> Dict[str, Any]:
        target = strategy_id or getattr(self.config, "stock_picker_default_strategy", "mainboard_swing_master")
        if target == "swing_after_close_picker":
            target = "mainboard_swing_master"
        for item in self._strategy_catalog():
            if item["strategy_id"] == target:
                return item
        raise ValueError(f"unknown strategy_id: {target}")

    def _resolve_scan_trade_date(self) -> date:
        if self.tushare_fetcher.is_available():
            try:
                trade_dates = self.tushare_fetcher._get_trade_dates()
                if trade_dates:
                    return datetime.strptime(trade_dates[0], "%Y%m%d").date()
            except Exception as exc:
                logger.warning("Stock picker failed to resolve Tushare trade date: %s", exc)
        latest_db_date = self._get_latest_daily_date()
        if latest_db_date is not None:
            return latest_db_date
        return date.today()

    def _load_mainboard_universe(self) -> List[Dict[str, str]]:
        stock_list = None
        if self.tushare_fetcher.is_available():
            try:
                stock_list = self.tushare_fetcher.get_stock_list()
            except Exception as exc:
                logger.warning("Stock picker failed to load universe from Tushare: %s", exc)

        if stock_list is None or stock_list.empty:
            try:
                stock_list = BaostockFetcher().get_stock_list()
            except Exception as exc:
                logger.warning("Stock picker failed to load universe from Baostock: %s", exc)

        if stock_list is None or stock_list.empty:
            stock_list = self._load_universe_from_local_db()

        if stock_list is None or stock_list.empty:
            raise RuntimeError(
                "unable to load A-share universe. Check Tushare token or local stock data availability."
            )
        rows: List[Dict[str, str]] = []
        for _, row in stock_list.iterrows():
            code = str(row.get("code") or "").strip()
            name = str(row.get("name") or "").strip()
            if not code or not name:
                continue
            if not self._is_main_board_code(code):
                continue
            if self._is_st_name(name):
                continue
            rows.append({"code": code, "name": name})
        return rows

    @staticmethod
    def _is_main_board_code(code: str) -> bool:
        return code.startswith(("600", "601", "603", "605", "000", "001", "002"))

    @staticmethod
    def _is_st_name(name: str) -> bool:
        normalized = name.replace(" ", "").upper()
        return normalized.startswith("ST") or normalized.startswith("*ST") or "ST" in normalized[:4]

    def _ensure_recent_market_data(
        self,
        scan_date: date,
        *,
        lookback_trading_days: int,
        force_refresh: bool = False,
    ) -> None:
        if not force_refresh and self._has_sufficient_local_history_window(
            scan_date,
            minimum_codes=2000,
            minimum_rows_per_code=min(lookback_trading_days, 60),
        ):
            logger.info("Stock picker reuses local daily snapshots for %s", scan_date)
            return

        universe = self._load_mainboard_universe()
        if self._refresh_recent_market_data_with_tickflow(
            scan_date=scan_date,
            universe=universe,
            lookback_trading_days=lookback_trading_days,
        ):
            return

        if not self.tushare_fetcher.is_available():
            if self._has_sufficient_local_history_window(scan_date, minimum_codes=2000, minimum_rows_per_code=40):
                logger.warning("Stock picker is using existing local history because Tushare is unavailable")
                return
            raise RuntimeError("TickFlow/Tushare are unavailable for full-market stock picker refresh")

        try:
            trade_dates = self.tushare_fetcher._get_trade_dates(end_date=scan_date.strftime("%Y%m%d"))[:lookback_trading_days]
        except Exception as exc:
            if self._has_recent_local_history(scan_date, minimum_codes=50):
                logger.warning("Stock picker is using existing local history because Tushare trade dates failed: %s", exc)
                return
            raise RuntimeError(
                f"Tushare token invalid or unavailable for stock picker refresh: {exc}"
            ) from exc
        if not trade_dates:
            raise RuntimeError("failed to resolve trade dates for stock picker")

        for trade_date in reversed(trade_dates):
            target_date = datetime.strptime(trade_date, "%Y%m%d").date()
            if not force_refresh and self._count_daily_rows(target_date) >= 2500:
                continue
            try:
                df = self.tushare_fetcher._call_api_with_rate_limit("daily", trade_date=trade_date)
            except Exception as exc:
                if self._count_daily_rows(target_date) >= 50:
                    logger.warning(
                        "Stock picker reuses local daily snapshot for %s because Tushare refresh failed: %s",
                        trade_date,
                        exc,
                    )
                    continue
                raise RuntimeError(
                    f"Tushare token invalid or unavailable for stock picker daily snapshot {trade_date}: {exc}"
                ) from exc
            normalized = self._normalize_market_daily(df)
            if normalized.empty:
                logger.warning("Stock picker daily snapshot empty for %s", trade_date)
                continue
            self._save_market_daily_snapshot(normalized)

    def _refresh_recent_market_data_with_tickflow(
        self,
        *,
        scan_date: date,
        universe: Sequence[Dict[str, str]],
        lookback_trading_days: int,
    ) -> bool:
        tickflow_fetcher = self.fetcher_manager._get_tickflow_fetcher()
        if tickflow_fetcher is None or not hasattr(tickflow_fetcher, "get_daily_batch"):
            return False

        code_list = [str(item.get("code") or "").strip() for item in universe if str(item.get("code") or "").strip()]
        if not code_list:
            return False

        try:
            batch_frames = tickflow_fetcher.get_daily_batch(
                code_list,
                count=lookback_trading_days,
                end_date=scan_date.strftime("%Y-%m-%d"),
            )
        except Exception as exc:
            logger.warning("Stock picker TickFlow batch refresh failed: %s", exc)
            return False

        if not batch_frames:
            return False

        normalized_frames: List[pd.DataFrame] = []
        for code, frame in batch_frames.items():
            if frame is None or frame.empty:
                continue
            normalized = frame.copy()
            normalized["code"] = code
            normalized["date"] = pd.to_datetime(normalized["date"]).dt.date
            normalized["data_source"] = "TickFlowFetcher"
            normalized_frames.append(
                normalized[["code", "date", "open", "high", "low", "close", "volume", "amount", "pct_chg", "data_source"]]
            )

        if not normalized_frames:
            return False

        merged = pd.concat(normalized_frames, ignore_index=True)
        self._save_market_daily_snapshot(merged)

        coverage = self._count_daily_rows(scan_date)
        logger.info(
            "Stock picker TickFlow batch refresh completed: scan_date=%s, rows=%s, coverage=%s",
            scan_date,
            len(merged),
            coverage,
        )
        return coverage >= 2000 and self._has_sufficient_local_history_window(
            scan_date,
            minimum_codes=1000,
            minimum_rows_per_code=min(lookback_trading_days, 40),
        )

    def _has_sufficient_local_history_window(
        self,
        scan_date: date,
        *,
        minimum_codes: int,
        minimum_rows_per_code: int,
    ) -> bool:
        window_start = scan_date - timedelta(days=max(minimum_rows_per_code * 2, 120))
        with self.db.get_session() as session:
            codes = session.execute(
                select(func.count())
                .select_from(
                    select(StockDaily.code)
                    .where(and_(StockDaily.date >= window_start, StockDaily.date <= scan_date))
                    .group_by(StockDaily.code)
                    .having(func.count(StockDaily.id) >= minimum_rows_per_code)
                    .subquery()
                )
            ).scalar_one_or_none()
        return int(codes or 0) >= minimum_codes

    def _get_latest_daily_date(self) -> Optional[date]:
        with self.db.get_session() as session:
            value = session.execute(select(func.max(StockDaily.date))).scalar_one_or_none()
            return value

    def _load_universe_from_local_db(self) -> pd.DataFrame:
        with self.db.get_session() as session:
            rows = session.execute(
                select(StockDaily.code, func.max(StockDaily.name).label("name"))
                .group_by(StockDaily.code)
            ).all()
        if not rows:
            return pd.DataFrame(columns=["code", "name"])
        return pd.DataFrame(
            [
                {
                    "code": str(code or "").strip(),
                    "name": str(name or code or "").strip(),
                }
                for code, name in rows
            ]
        )

    def _has_recent_local_history(self, scan_date: date, minimum_codes: int = 50) -> bool:
        window_start = scan_date - timedelta(days=120)
        with self.db.get_session() as session:
            codes = session.execute(
                select(func.count(func.distinct(StockDaily.code))).where(
                    and_(StockDaily.date >= window_start, StockDaily.date <= scan_date)
                )
            ).scalar_one_or_none()
        return int(codes or 0) >= minimum_codes

    def _count_daily_rows(self, target_date: date) -> int:
        with self.db.get_session() as session:
            count = session.execute(
                select(func.count(StockDaily.id)).where(StockDaily.date == target_date)
            ).scalar() or 0
            return int(count)

    @staticmethod
    def _normalize_market_daily(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        normalized = df.copy()
        normalized["code"] = normalized["ts_code"].astype(str).str.split(".").str[0]
        normalized["date"] = pd.to_datetime(normalized["trade_date"], format="%Y%m%d").dt.date
        normalized["volume"] = pd.to_numeric(normalized["vol"], errors="coerce") * 100
        normalized["amount"] = pd.to_numeric(normalized["amount"], errors="coerce") * 1000
        for column in ("open", "high", "low", "close", "pct_chg"):
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
        normalized["data_source"] = "TushareFetcher"
        return normalized[["code", "date", "open", "high", "low", "close", "volume", "amount", "pct_chg", "data_source"]]

    def _save_market_daily_snapshot(self, df: pd.DataFrame) -> None:
        if df.empty:
            return

        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            if not row.get("code") or row.get("date") is None:
                continue
            records.append(
                {
                    "code": str(row["code"]),
                    "date": row["date"],
                    "open": self._to_float(row.get("open")),
                    "high": self._to_float(row.get("high")),
                    "low": self._to_float(row.get("low")),
                    "close": self._to_float(row.get("close")),
                    "volume": self._to_float(row.get("volume")),
                    "amount": self._to_float(row.get("amount")),
                    "pct_chg": self._to_float(row.get("pct_chg")),
                    "ma5": None,
                    "ma10": None,
                    "ma20": None,
                    "volume_ratio": None,
                    "data_source": str(row.get("data_source") or "TushareFetcher"),
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                }
            )
        if not records:
            return

        def _write(session) -> int:
            table = StockDaily.__table__
            # SQLite has a low parameter ceiling, so large market-wide snapshots
            # must be upserted in small chunks.
            chunk_size = 50
            for start in range(0, len(records), chunk_size):
                batch = records[start:start + chunk_size]
                stmt = sqlite_insert(table).values(batch)
                update_cols = {
                    column: getattr(stmt.excluded, column)
                    for column in (
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "amount",
                        "pct_chg",
                        "data_source",
                        "updated_at",
                    )
                }
                session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=["code", "date"],
                        set_=update_cols,
                    )
                )
            return len(records)

        self.db._run_write_transaction("save_stock_picker_market_snapshot", _write)

    def _load_history_frames(
        self,
        codes: Sequence[str],
        scan_date: date,
        lookback_days: int = 90,
    ) -> Dict[str, pd.DataFrame]:
        if not codes:
            return {}
        start_date = scan_date - timedelta(days=lookback_days * 2)
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        code_list = list(dict.fromkeys(codes))
        batch_size = 800
        with self.db.get_session() as session:
            for index in range(0, len(code_list), batch_size):
                batch = code_list[index:index + batch_size]
                rows = session.execute(
                    select(StockDaily)
                    .where(
                        and_(
                            StockDaily.code.in_(batch),
                            StockDaily.date >= start_date,
                            StockDaily.date <= scan_date,
                        )
                    )
                    .order_by(StockDaily.code.asc(), StockDaily.date.asc())
                ).scalars().all()
                for row in rows:
                    grouped.setdefault(row.code, []).append(
                        {
                            "date": row.date,
                            "open": self._to_float(row.open),
                            "high": self._to_float(row.high),
                            "low": self._to_float(row.low),
                            "close": self._to_float(row.close),
                            "volume": self._to_float(row.volume),
                            "amount": self._to_float(row.amount),
                            "pct_chg": self._to_float(row.pct_chg),
                        }
                    )
        frames: Dict[str, pd.DataFrame] = {}
        for code, items in grouped.items():
            df = pd.DataFrame(items)
            if df.empty:
                continue
            df = df.sort_values("date").reset_index(drop=True)
            frames[code] = self._attach_indicators(df)
        return frames

    @staticmethod
    def _attach_indicators(df: pd.DataFrame) -> pd.DataFrame:
        bars = df.copy()
        numeric_columns = ["open", "high", "low", "close", "volume", "amount", "pct_chg"]
        for column in numeric_columns:
            bars[column] = pd.to_numeric(bars[column], errors="coerce")
        for window in (5, 10, 20, 30, 60):
            bars[f"ma{window}"] = bars["close"].rolling(window).mean()
        bars["vol_ma5"] = bars["volume"].rolling(5).mean()
        bars["vol_ma20"] = bars["volume"].rolling(20).mean()
        bars["ma20_slope"] = bars["ma20"] - bars["ma20"].shift(1)
        bars["ma60_slope"] = bars["ma60"] - bars["ma60"].shift(1)
        bars["max_high_20_prev"] = bars["high"].shift(1).rolling(20).max()
        low_n = bars["low"].rolling(9).min()
        high_n = bars["high"].rolling(9).max()
        rsv = (bars["close"] - low_n) / (high_n - low_n).replace(0, math.nan) * 100
        bars["kdj_k"] = rsv.ewm(com=2, adjust=False).mean()
        bars["kdj_d"] = bars["kdj_k"].ewm(com=2, adjust=False).mean()
        bars["kdj_j"] = 3 * bars["kdj_k"] - 2 * bars["kdj_d"]
        ema12 = bars["close"].ewm(span=12, adjust=False).mean()
        ema26 = bars["close"].ewm(span=26, adjust=False).mean()
        bars["macd_dif"] = ema12 - ema26
        bars["macd_dea"] = bars["macd_dif"].ewm(span=9, adjust=False).mean()
        bars["macd_hist"] = (bars["macd_dif"] - bars["macd_dea"]) * 2
        return bars

    def _evaluate_strategy(
        self,
        strategy_id: str,
        *,
        code: str,
        name: str,
        bars: pd.DataFrame,
        market_snapshot: Dict[str, Any],
        params: Dict[str, Any],
    ) -> StrategySignal:
        if len(bars) < 65:
            return StrategySignal(False, 0.0, "insufficient", "观望", "样本不足", [], None, None, {})
        today = bars.iloc[-1]
        prev = bars.iloc[-2]
        recent8 = bars.tail(8)

        if any(pd.isna(today.get(col)) for col in ("ma5", "ma10", "ma20", "ma30", "ma60")):
            return StrategySignal(False, 0.0, "insufficient", "观望", "均线样本不足", [], None, None, {})

        close_price = self._to_float(today["close"])
        ma5 = self._to_float(today["ma5"])
        ma10 = self._to_float(today["ma10"])
        ma20 = self._to_float(today["ma20"])
        ma30 = self._to_float(today["ma30"])
        ma60 = self._to_float(today["ma60"])
        low_price = self._to_float(today["low"])
        volume = self._to_float(today["volume"])
        vol_ma20 = max(self._to_float(today["vol_ma20"]), 1.0)
        vol_ma5 = max(self._to_float(today["vol_ma5"]), 1.0)
        distance_to_ma20_pct = abs(close_price - ma20) / max(ma20, 0.01) * 100
        distance_to_ma5_pct = abs(close_price - ma5) / max(ma5, 0.01) * 100
        volume_spike_factor = float((recent8["volume"] / recent8["vol_ma20"].replace(0, pd.NA)).max(skipna=True) or 0.0)
        ma5_cross_ma20 = bool(prev["ma5"] <= prev["ma20"] and today["ma5"] > today["ma20"])
        ma5_cross_recent = bool((bars["ma5"].shift(1) <= bars["ma20"].shift(1)).tail(2).any() and (bars["ma5"] > bars["ma20"]).tail(2).any())
        ma20_up = bool(today["ma20_slope"] > 0)
        ma60_up = bool(today["ma60_slope"] > 0)
        trend_stack = bool(ma5 > ma10 > ma20 > ma30)
        trend_stack_up = bool(
            trend_stack
            and today["ma5"] > prev["ma5"]
            and today["ma10"] > prev["ma10"]
            and today["ma20"] > prev["ma20"]
            and today["ma30"] > prev["ma30"]
        )
        touch_ma5 = bool(low_price <= ma5 * 1.01 and close_price >= ma5 * 0.99 and close_price >= ma10 * 0.985)
        shrink_volume = bool(volume <= vol_ma5 * 0.95)
        breakout_near_high = bool(close_price >= self._to_float(today["max_high_20_prev"]) * 0.985 if not pd.isna(today["max_high_20_prev"]) else False)
        macd_bull = bool(today["macd_dif"] > today["macd_dea"] and today["macd_hist"] > -0.02)
        kdj_bull = bool(today["kdj_j"] >= today["kdj_k"])
        strong_body = bool(self._to_float(today["pct_chg"]) >= 2.0)

        reasons: List[str] = []
        score = 0.0
        passed = False
        setup_type = "mixed"

        if strategy_id in {"mainboard_swing_master", "ma_golden_cross"}:
            breakout_mode = (
                ma5_cross_recent
                and ma20_up
                and ma60_up
                and distance_to_ma20_pct <= float(params.get("max_ma20_distance_pct", 3.0))
                and volume_spike_factor >= float(params.get("volume_spike_multiplier", 1.8))
                and close_price >= ma20
            )
            if breakout_mode:
                setup_type = "breakout"
                score += 35
                reasons.append("5日线近期上穿20日线，20日与60日线同步拐头向上")
            if ma5_cross_ma20:
                score += 8
                reasons.append("当日形成明确金叉")
            if strong_body:
                score += 8
                reasons.append("收盘中阳确认启动")
            if breakout_near_high:
                score += 10
                reasons.append("接近近端突破位收盘")
            if macd_bull:
                score += 8
                reasons.append("MACD 多头")
            if kdj_bull:
                score += 6
            passed = breakout_mode

        if strategy_id in {"mainboard_swing_master", "shrink_pullback"}:
            pullback_mode = (
                trend_stack_up
                and touch_ma5
                and ma60_up
                and volume_spike_factor >= float(params.get("volume_spike_multiplier", 1.5))
            )
            if pullback_mode:
                setup_type = "pullback" if strategy_id != "mainboard_swing_master" or not passed else "mixed"
                score += 34
                reasons.append("5/10/20/30日线多头向上，K线回踩5日线后仍守住趋势")
            if distance_to_ma5_pct <= 2.0:
                score += 9
            if shrink_volume:
                score += 8
                reasons.append("回踩阶段量能收缩，抛压可控")
            if macd_bull:
                score += 8
            if kdj_bull:
                score += 5
            passed = passed or pullback_mode

        if strategy_id == "volume_breakout":
            breakout_mode = breakout_near_high and volume_spike_factor >= float(params.get("volume_spike_multiplier", 2.0)) and ma20_up and ma60_up
            if breakout_mode:
                setup_type = "breakout"
                score += 42
                reasons.append("放量突破近20日高点")
                passed = True
            if strong_body:
                score += 12
            if macd_bull:
                score += 10

        if ma60_up:
            score += 8
        if market_snapshot.get("score", 50) >= 55:
            score += 6

        min_score_threshold = float(params.get("min_score_threshold", 70.0))
        passed = passed and score >= min_score_threshold

        if not passed:
            return StrategySignal(
                passed=False,
                score=score,
                setup_type=setup_type,
                operation_advice="观望",
                analysis_summary=f"{name} 未达到 {strategy_id} 触发阈值",
                reasons=reasons,
                stop_loss=None,
                take_profit=None,
                metrics={
                    "distance_to_ma20_pct": round(distance_to_ma20_pct, 2),
                    "distance_to_ma5_pct": round(distance_to_ma5_pct, 2),
                    "volume_spike_factor": round(volume_spike_factor, 2),
                    "trend_stack_up": trend_stack_up,
                    "ma20_up": ma20_up,
                    "ma60_up": ma60_up,
                },
            )

        stop_loss = round(min(ma10 if setup_type == "pullback" else ma20, close_price * 0.95), 2)
        take_profit = round(close_price * (1.12 if setup_type == "pullback" else 1.10), 2)

        return StrategySignal(
            passed=True,
            score=score,
            setup_type=setup_type,
            operation_advice="买入",
            analysis_summary=f"{name} 符合 {setup_type} 型波段候选，适合次日观察承接后参与。",
            reasons=reasons[:6],
            stop_loss=stop_loss,
            take_profit=take_profit,
            metrics={
                "distance_to_ma20_pct": round(distance_to_ma20_pct, 2),
                "distance_to_ma5_pct": round(distance_to_ma5_pct, 2),
                "volume_spike_factor": round(volume_spike_factor, 2),
                "trend_stack_up": trend_stack_up,
                "ma20_up": ma20_up,
                "ma60_up": ma60_up,
                "macd_bull": macd_bull,
                "kdj_bull": kdj_bull,
            },
        )

    def _build_market_snapshot(self, *, use_cache: bool = True) -> Dict[str, Any]:
        indices = self.fetcher_manager.get_main_indices(region="cn", use_cache=use_cache) or []
        stats = self.fetcher_manager.get_market_stats(use_cache=use_cache) or {}
        score = 50.0
        rising_indices = 0
        for item in indices:
            pct = self._to_float(item.get("change_pct"))
            if pct > 0:
                rising_indices += 1
                score += 5
            elif pct < 0:
                score -= 4
        up_count = float(stats.get("up_count") or 0)
        down_count = float(stats.get("down_count") or 0)
        limit_up_count = float(stats.get("limit_up_count") or 0)
        limit_down_count = float(stats.get("limit_down_count") or 0)
        if up_count + down_count > 0:
            breadth = up_count / max(up_count + down_count, 1.0)
            score += (breadth - 0.5) * 24
        score += min(limit_up_count, 80.0) * 0.08
        score -= min(limit_down_count, 40.0) * 0.25
        regime = "震荡"
        if score >= 63:
            regime = "偏强"
        elif score <= 45:
            regime = "偏弱"
        return {
            "score": round(max(0.0, min(score, 100.0)), 2),
            "regime": regime,
            "indices": indices,
            "stats": stats,
            "summary": f"A股情绪 {regime}，指数走强数 {rising_indices}/{len(indices) or 1}。",
        }

    def _build_us_market_snapshot(self) -> Dict[str, Any]:
        indices = self.fetcher_manager.get_main_indices(region="us") or []
        score = 50.0
        falling = 0
        for item in indices:
            pct = self._to_float(item.get("change_pct"))
            if pct > 0:
                score += 6
            elif pct < 0:
                falling += 1
                score -= 7
        mood = "中性"
        if score >= 58:
            mood = "偏暖"
        elif score <= 42:
            mood = "偏冷"
        return {
            "score": round(max(0.0, min(score, 100.0)), 2),
            "mood": mood,
            "indices": indices,
            "summary": f"隔夜美股情绪 {mood}，弱势指数数 {falling}/{len(indices) or 1}。",
        }

    def _collect_candidate_intel(
        self,
        *,
        code: str,
        name: str,
        scan_date: Optional[date] = None,
        query_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        effective_scan_date = scan_date or date.today()
        cached_results = self._load_cached_candidate_intel(
            code=code,
            name=name,
            scan_date=effective_scan_date,
        )
        if cached_results:
            news_context = (
                self.search_service.format_intel_report(cached_results, name)
                if self.search_service is not None
                else self._format_cached_intel_report(cached_results, name)
            )
            return {
                "intel_results": cached_results,
                "news_context": news_context,
                "news_score": self._score_intel_payload(cached_results),
                "from_cache": True,
            }

        if self.search_service is None or not getattr(self.search_service, "is_available", False):
            return {"news_context": None, "news_score": 50.0, "from_cache": False}

        try:
            intel_results = self.search_service.search_comprehensive_intel(code, name, max_searches=4)
            self._save_candidate_intel_cache(
                code=code,
                name=name,
                intel_results=intel_results,
                query_id=query_id,
            )
            news_context = self.search_service.format_intel_report(intel_results, name)
            news_score = self._score_intel_payload(intel_results)
            return {
                "intel_results": intel_results,
                "news_context": news_context,
                "news_score": news_score,
                "from_cache": False,
            }
        except Exception as exc:
            logger.warning("Stock picker intel collection failed for %s: %s", code, exc)
            return {"news_context": None, "news_score": 50.0, "from_cache": False}

    def _load_cached_candidate_intel(
        self,
        *,
        code: str,
        name: str,
        scan_date: date,
    ) -> Dict[str, SearchResponse]:
        window_start = datetime.combine(scan_date, dt_time.min)
        window_end = window_start + timedelta(days=1)
        grouped: Dict[str, List[NewsIntel]] = {}
        with self.db.get_session() as session:
            rows = session.execute(
                select(NewsIntel)
                .where(
                    and_(
                        NewsIntel.code == code,
                        NewsIntel.fetched_at >= window_start,
                        NewsIntel.fetched_at < window_end,
                    )
                )
                .order_by(NewsIntel.dimension.asc(), desc(NewsIntel.published_date), desc(NewsIntel.fetched_at))
            ).scalars().all()
        for row in rows:
            grouped.setdefault(str(row.dimension or "latest_news"), [])
            if len(grouped[str(row.dimension or "latest_news")]) >= 5:
                continue
            grouped[str(row.dimension or "latest_news")].append(row)

        responses: Dict[str, SearchResponse] = {}
        for dimension, items in grouped.items():
            if not items:
                continue
            responses[dimension] = SearchResponse(
                query=items[0].query or f"{name} {code}",
                provider=items[0].provider or "cache",
                success=True,
                search_time=0.0,
                results=[
                    SearchResult(
                        title=item.title or "",
                        snippet=item.snippet or "",
                        url=item.url or "",
                        source=item.source or (item.provider or "cache"),
                        published_date=item.published_date.isoformat() if item.published_date else None,
                    )
                    for item in items
                ],
            )
        return responses

    def _save_candidate_intel_cache(
        self,
        *,
        code: str,
        name: str,
        intel_results: Dict[str, SearchResponse],
        query_id: Optional[str],
    ) -> None:
        for dimension, response in intel_results.items():
            if not response.success or not response.results:
                continue
            try:
                self.db.save_news_intel(
                    code=code,
                    name=name,
                    dimension=dimension,
                    query=response.query,
                    response=response,
                    query_context={
                        "query_id": query_id or "",
                        "query_source": "system",
                        "requester_platform": "picker",
                        "requester_query": response.query,
                    },
                )
            except Exception as exc:
                logger.warning("Stock picker failed to cache news intel for %s/%s: %s", code, dimension, exc)

    @staticmethod
    def _format_cached_intel_report(intel_results: Dict[str, SearchResponse], stock_name: str) -> str:
        lines = [f"## {stock_name} 消息面摘要", ""]
        for dimension, response in intel_results.items():
            if not response.results:
                continue
            lines.append(f"### {dimension}")
            for item in response.results[:3]:
                lines.append(f"- {item.title}")
            lines.append("")
        return "\n".join(lines).strip()


def _picker_format_price(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "-"
    if numeric <= 0:
        return "-"
    return f"{numeric:.2f}"


def _picker_format_pct(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{numeric:.2f}%"


def _picker_build_strategy_catalog() -> List[Dict[str, Any]]:
    skills = load_skills_from_directory(_BUILTIN_STRATEGY_DIR)
    custom_mainboard = {
        "strategy_id": "mainboard_swing_master",
        "name": "主力波段双模",
        "description": "收盘后筛选沪深主板非 ST 标的，综合突破启动、趋势回踩、量能、消息面与市场情绪。",
        "skill_id": "swing_after_close_picker",
        "category": "swing",
        "params": dict(_PICKER_STRATEGY_PRESETS["mainboard_swing_master"]["params"]),
        "priority": int(_PICKER_STRATEGY_PRESETS["mainboard_swing_master"]["priority"]),
    }
    catalog: List[Dict[str, Any]] = [custom_mainboard]
    seen = {"mainboard_swing_master"}

    for skill in skills:
        strategy_id = "mainboard_swing_master" if skill.name == "swing_after_close_picker" else skill.name
        if strategy_id in seen:
            continue
        preset = _PICKER_STRATEGY_PRESETS.get(strategy_id)
        if not preset:
            continue
        catalog.append(
            {
                "strategy_id": strategy_id,
                "name": skill.display_name or strategy_id,
                "description": skill.description or strategy_id,
                "skill_id": skill.name,
                "category": skill.category or "trend",
                "params": dict(preset.get("params") or {}),
                "priority": int(preset.get("priority") or 999),
            }
        )
        seen.add(strategy_id)

    catalog.sort(key=lambda item: (int(item.get("priority") or 999), str(item["strategy_id"])))
    return [
        {
            "strategy_id": item["strategy_id"],
            "name": item["name"],
            "description": item["description"],
            "skill_id": item["skill_id"],
            "category": item["category"],
            "params": item["params"],
        }
        for item in catalog
    ]


def _picker_strategy_catalog(self: StockPickerService) -> List[Dict[str, Any]]:
    return _picker_build_strategy_catalog()


def _picker_list_strategies() -> List[Dict[str, Any]]:
    return _picker_build_strategy_catalog()


def _picker_to_float(self: StockPickerService, value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _picker_score_intel_payload(self: StockPickerService, intel_results: Dict[str, SearchResponse]) -> float:
    positive_keywords = ("订单", "中标", "预增", "增长", "回购", "增持", "景气", "上调", "催化", "突破")
    negative_keywords = ("减持", "立案", "处罚", "亏损", "诉讼", "风险", "暴雷", "问询", "质押", "违约")
    score = 50.0
    for response in intel_results.values():
        if not response.success:
            continue
        for item in response.results[:3]:
            text = f"{item.title} {item.snippet}"
            score += sum(2.5 for keyword in positive_keywords if keyword in text)
            score -= sum(4.0 for keyword in negative_keywords if keyword in text)
    return max(0.0, min(score, 100.0))


def _picker_ensure_candidate_metrics(self: StockPickerService, candidate: Dict[str, Any]) -> Dict[str, Any]:
    metrics = dict(candidate.get("metrics") or {})
    required_keys = {"close", "ma5", "ma10", "ma20", "ma30", "ma60", "support_price", "resistance_price", "atr14"}
    if required_keys.issubset({key for key, value in metrics.items() if value not in (None, "")}):
        return metrics

    code = str(candidate.get("code") or "").strip()
    scan_date_raw = candidate.get("scan_date")
    if not code or not scan_date_raw:
        return metrics
    try:
        scan_date = scan_date_raw if isinstance(scan_date_raw, date) else datetime.fromisoformat(str(scan_date_raw)).date()
    except ValueError:
        return metrics

    bars = self._load_history_frames([code], scan_date, lookback_days=90).get(code)
    if bars is None or bars.empty:
        return metrics

    last = bars.iloc[-1]
    recent10 = bars.tail(10)
    recent20 = bars.tail(20)
    prev_close_series = bars["close"].shift(1)
    tr = pd.concat(
        [
            bars["high"] - bars["low"],
            (bars["high"] - prev_close_series).abs(),
            (bars["low"] - prev_close_series).abs(),
        ],
        axis=1,
    ).max(axis=1)
    metrics.update(
        {
            "close": round(self._to_float(last.get("close")), 2),
            "open": round(self._to_float(last.get("open")), 2),
            "high": round(self._to_float(last.get("high")), 2),
            "low": round(self._to_float(last.get("low")), 2),
            "prev_close": round(self._to_float(bars.iloc[-2]["close"]) if len(bars) > 1 else last.get("close"), 2),
            "ma5": round(self._to_float(last.get("ma5")), 2),
            "ma10": round(self._to_float(last.get("ma10")), 2),
            "ma20": round(self._to_float(last.get("ma20")), 2),
            "ma30": round(self._to_float(last.get("ma30")), 2),
            "ma60": round(self._to_float(last.get("ma60")), 2),
            "recent10_high": round(self._to_float(recent10["high"].max()), 2),
            "recent10_low": round(self._to_float(recent10["low"].min()), 2),
            "recent20_high": round(self._to_float(recent20["high"].max()), 2),
            "recent20_low": round(self._to_float(recent20["low"].min()), 2),
            "return_5d_pct": round(((self._to_float(last.get("close")) / max(self._to_float(bars.iloc[-6]["close"]) if len(bars) > 5 else self._to_float(last.get("close")), 0.01)) - 1) * 100, 2) if len(bars) > 5 else 0.0,
            "return_10d_pct": round(((self._to_float(last.get("close")) / max(self._to_float(bars.iloc[-11]["close"]) if len(bars) > 10 else self._to_float(last.get("close")), 0.01)) - 1) * 100, 2) if len(bars) > 10 else 0.0,
            "avg_amp_10_pct": round((((recent10["high"] - recent10["low"]) / recent10["close"].replace(0, math.nan)) * 100).mean(), 2),
            "atr14": round(self._to_float(tr.tail(14).mean()), 2),
            "support_price": round(max(min(self._to_float(last.get("ma5")), self._to_float(last.get("ma10")), self._to_float(last.get("ma20"))), self._to_float(recent10["low"].min())), 2),
            "resistance_price": round(max(self._to_float(recent10["high"].max()), self._to_float(recent20["high"].max())), 2),
            "breakout_trigger_price": round(max(self._to_float(recent10["high"].max()) * 1.002, self._to_float(last.get("close")) * 1.005), 2),
        }
    )
    candidate["metrics"] = metrics
    return metrics


def _picker_derive_trade_levels(self: StockPickerService, candidate: Dict[str, Any]) -> Dict[str, float]:
    metrics = _picker_ensure_candidate_metrics(self, candidate)
    close_price = max(self._to_float(metrics.get("close")), 0.01)
    ma5 = self._to_float(metrics.get("ma5"))
    ma10 = self._to_float(metrics.get("ma10"))
    ma20 = self._to_float(metrics.get("ma20"))
    atr14 = max(self._to_float(metrics.get("atr14")), close_price * 0.012, 0.05)
    support_price = max(self._to_float(metrics.get("support_price")), close_price * 0.96, 0.01)
    resistance_price = max(self._to_float(metrics.get("resistance_price")), close_price * 1.015)
    breakout_trigger = max(self._to_float(metrics.get("breakout_trigger_price")), resistance_price * 1.001, close_price)
    setup_type = str(candidate.get("setup_type") or "pullback").strip().lower()

    if setup_type == "breakout":
        buy_low = max(breakout_trigger * 0.998, close_price * 1.002)
        buy_high = buy_low + max(atr14 * 0.35, close_price * 0.004)
    else:
        anchor = ma5 if ma5 > 0 else close_price
        buy_low = max(min(anchor * 0.998, close_price * 0.997), support_price)
        buy_high = min(max(anchor * 1.005, close_price * 1.003), resistance_price * 0.998)

    stop_loss = self._to_float(candidate.get("stop_loss")) or max(min(ma10 or close_price, ma20 or close_price), support_price * 0.995)
    take_profit = self._to_float(candidate.get("take_profit")) or max(resistance_price * 1.03, close_price * 1.08)
    reduce_price = min(max(close_price + atr14 * 1.5, resistance_price * 0.995), take_profit * 0.985)
    abandon_price = min(stop_loss, support_price * 0.995)

    buy_low = max(buy_low, 0.01)
    buy_high = max(buy_high, buy_low)
    breakout_buy = max(breakout_trigger, buy_high, 0.01)
    reduce_price = max(reduce_price, breakout_buy)
    take_profit = max(take_profit, reduce_price)
    stop_loss = max(stop_loss, 0.01)
    abandon_price = max(min(abandon_price, stop_loss), 0.01)

    return {
        "buy_low": round(buy_low, 2),
        "buy_high": round(buy_high, 2),
        "breakout_buy": round(breakout_buy, 2),
        "reduce_price": round(reduce_price, 2),
        "take_profit": round(take_profit, 2),
        "stop_loss": round(stop_loss, 2),
        "abandon_price": round(abandon_price, 2),
    }


def _picker_build_stock_profile(self: StockPickerService, candidate: Dict[str, Any]) -> str:
    metrics = _picker_ensure_candidate_metrics(self, candidate)
    setup_label = "趋势回踩" if str(candidate.get("setup_type") or "").lower() == "pullback" else "突破启动"
    return (
        f"{setup_label}结构，收盘 {self._format_price(metrics.get('close'))}，"
        f"MA5/10/20/30/60="
        f"{self._format_price(metrics.get('ma5'))}/"
        f"{self._format_price(metrics.get('ma10'))}/"
        f"{self._format_price(metrics.get('ma20'))}/"
        f"{self._format_price(metrics.get('ma30'))}/"
        f"{self._format_price(metrics.get('ma60'))}；"
        f"近 8 日最大倍量 {metrics.get('volume_spike_factor', '-')} 倍，"
        f"5 日涨幅 {_picker_format_pct(metrics.get('return_5d_pct'))}，"
        f"10 日涨幅 {_picker_format_pct(metrics.get('return_10d_pct'))}，"
        f"10 日均振幅 {_picker_format_pct(metrics.get('avg_amp_10_pct'))}，"
        f"短支撑 {self._format_price(metrics.get('support_price'))}，"
        f"短压 {self._format_price(metrics.get('resistance_price'))}，"
        f"ATR14 {self._format_price(metrics.get('atr14'))}。"
    )


def _picker_is_action_plan_usable(text: Optional[str]) -> bool:
    content = (text or "").strip()
    if len(content) < 180:
        return False
    required_sections = ("一、个股画像", "二、入场前提", "三、三种开盘预案", "四、挂单计划", "五、持仓与卖出", "六、风险点")
    if not all(section in content for section in required_sections):
        return False
    required_prices = ("低吸挂单价", "接力挂单价", "突破追价", "首次减仓价", "止盈价", "止损价", "放弃价")
    return all(label in content for label in required_prices)


def _picker_build_template_action_plan(
    self: StockPickerService,
    *,
    candidate: Dict[str, Any],
    strategy: Dict[str, Any],
    market_snapshot: Dict[str, Any],
    us_snapshot: Dict[str, Any],
    review_text: Optional[str],
) -> str:
    trade_levels = self._derive_trade_levels(candidate)
    stock_profile = self._build_stock_profile(candidate)
    reasons = "；".join(str(item) for item in (candidate.get("reasons") or [])[:6]) or "量价结构满足策略条件"
    return (
        f"### {candidate['name']}（{candidate['code']}）次日操作手册\n\n"
        f"**核心策略：** {strategy['name']} | **形态：** {candidate.get('setup_type')} | **评分：** {candidate.get('score')}\n\n"
        f"**一、个股画像**\n"
        f"- {stock_profile}\n"
        f"- 量化结论：{candidate.get('analysis_summary')}\n"
        f"- 触发原因：{reasons}\n"
        f"- 市场环境：A股 {market_snapshot.get('summary') or market_snapshot.get('regime') or '中性'}；美股 {us_snapshot.get('summary') or us_snapshot.get('mood') or '中性'}\n"
        f"- 消息面：{review_text or '当前没有额外高置信新闻，先按中性处理。'}\n\n"
        f"**二、入场前提**\n"
        f"- 开盘后先看是否稳住 {self._format_price(trade_levels['buy_low'])} 上方，弱于该价位不急于挂单。\n"
        f"- 若竞价或开盘 15 分钟内直接跌破 {self._format_price(trade_levels['abandon_price'])}，当天放弃。\n\n"
        f"**三、三种开盘预案**\n"
        f"- 平开或小低开：优先观察 {self._format_price(trade_levels['buy_low'])}-{self._format_price(trade_levels['buy_high'])} 的承接，缩量回踩可低吸。\n"
        f"- 小幅高开：高开后不破 {self._format_price(trade_levels['buy_high'])}，可等二次站稳再挂单。\n"
        f"- 强势突破：只有放量站上 {self._format_price(trade_levels['breakout_buy'])} 才允许追价，而且只做轻仓。\n\n"
        f"**四、挂单计划**\n"
        f"- 低吸挂单价：{self._format_price(trade_levels['buy_low'])}\n"
        f"- 接力挂单价：{self._format_price(trade_levels['buy_high'])}\n"
        f"- 突破追价：{self._format_price(trade_levels['breakout_buy'])}\n"
        f"- 首次减仓价：{self._format_price(trade_levels['reduce_price'])}\n"
        f"- 止盈价：{self._format_price(trade_levels['take_profit'])}\n"
        f"- 止损价：{self._format_price(trade_levels['stop_loss'])}\n"
        f"- 放弃价：{self._format_price(trade_levels['abandon_price'])}\n\n"
        f"**五、持仓与卖出**\n"
        f"- 计划持有 5-10 个交易日，2-3 天内站不上 {self._format_price(trade_levels['reduce_price'])} 就降低预期。\n"
        f"- 触及 {self._format_price(trade_levels['reduce_price'])} 可先减仓锁定利润，接近 {self._format_price(trade_levels['take_profit'])} 再分批兑现。\n"
        f"- 放量跌破 {self._format_price(trade_levels['stop_loss'])} 直接执行止损，不等反抽。\n\n"
        f"**六、风险点**\n"
        f"- 若板块龙头转弱、指数情绪急速回落、或新闻出现明确利空，优先降仓位。\n"
    )


def _picker_build_action_plan(
    self: StockPickerService,
    *,
    candidate: Dict[str, Any],
    strategy: Dict[str, Any],
    market_snapshot: Dict[str, Any],
    us_snapshot: Dict[str, Any],
    review_payload: Optional[Dict[str, Any]],
) -> tuple[str, Optional[str]]:
    review_text = review_payload.get("news_context") if isinstance(review_payload, dict) else None
    trade_levels = self._derive_trade_levels(candidate)
    stock_profile = self._build_stock_profile(candidate)
    fallback = self._build_template_action_plan(
        candidate=candidate,
        strategy=strategy,
        market_snapshot=market_snapshot,
        us_snapshot=us_snapshot,
        review_text=review_text,
    )
    if not getattr(self.analyzer, "is_available", lambda: False)():
        return fallback, None

    prompt = (
        "你是 A 股收盘后波段交易助手。请根据下面这只股票的量化数据、均线形态、波动特征、支撑压力位和新闻摘要，"
        "生成一份面向第二天的操作手册。必须说清楚挂什么价格买、挂什么价格卖，并且要体现这只股票自己的历史特征。"
        "\n\n输出要求：\n"
        "1. 只能用中文。\n"
        "2. 必须输出以下标题：一、个股画像；二、入场前提；三、三种开盘预案；四、挂单计划；五、持仓与卖出；六、风险点。\n"
        "3. 挂单计划里必须出现具体价格：低吸挂单价、接力挂单价、突破追价、首次减仓价、止盈价、止损价、放弃价。\n"
        "4. 不能写成泛化模板，必须解释该股的均线结构、量能、波动率、支撑压力为何对应这些价格。\n"
        "5. 不能编造未给出的基本面或新闻。\n\n"
        f"策略：{strategy['name']}\n"
        f"股票：{candidate['name']} ({candidate['code']})\n"
        f"形态：{candidate.get('setup_type')}\n"
        f"评分：{candidate.get('score')}\n"
        f"量化结论：{candidate.get('analysis_summary')}\n"
        f"触发原因：{'；'.join(str(item) for item in (candidate.get('reasons') or [])[:6])}\n"
        f"个股画像：{stock_profile}\n"
        f"A股情绪：{market_snapshot.get('summary') or market_snapshot.get('regime') or '中性'}\n"
        f"美股情绪：{us_snapshot.get('summary') or us_snapshot.get('mood') or '中性'}\n"
        f"价格参考：低吸 {trade_levels['buy_low']}，接力 {trade_levels['buy_high']}，突破 {trade_levels['breakout_buy']}，"
        f"减仓 {trade_levels['reduce_price']}，止盈 {trade_levels['take_profit']}，止损 {trade_levels['stop_loss']}，放弃 {trade_levels['abandon_price']}\n"
        f"消息摘要：{review_text or '暂无额外高置信新闻，按中性处理。'}"
    )
    generated = self.analyzer.generate_text(
        prompt,
        max_tokens=1400,
        temperature=0.35,
        timeout_seconds=25,
    )
    if not _picker_is_action_plan_usable(generated):
        return fallback, None
    return generated, getattr(self.config, "litellm_model", None)


def _picker_build_run_summary_markdown(
    self: StockPickerService,
    strategy: Dict[str, Any],
    scan_date: date,
    selected: Sequence[Dict[str, Any]],
    market_snapshot: Dict[str, Any],
    us_snapshot: Dict[str, Any],
) -> str:
    lines = [
        f"# {scan_date.isoformat()} 收盘后选股",
        "",
        f"- 策略：{strategy['name']}",
        "- 范围：沪深主板（非 ST）",
        f"- A股情绪：{market_snapshot.get('summary') or market_snapshot.get('regime') or '中性'}",
        f"- 美股情绪：{us_snapshot.get('summary') or us_snapshot.get('mood') or '中性'}",
        f"- 候选上限：{_MAX_SELECTED_CANDIDATES} 只，详细手册上限：{_MAX_REPORT_CANDIDATES} 只",
        "",
        "## 候选股",
    ]
    if not selected:
        lines.append("- 今日没有符合条件的标的。")
        return "\n".join(lines)
    for item in selected:
        lines.append(
            f"- {item['rank']}. {item['name']}({item['code']}) | 分数 {item['score']} | {item['setup_type']} | {item['analysis_summary']}"
        )
    return "\n".join(lines)


def _picker_send_scan_notification(self: StockPickerService, payload: Dict[str, Any]) -> None:
    try:
        NotificationService().send(self._build_notification_markdown(payload))
    except Exception as exc:
        logger.warning("Stock picker notification failed: %s", exc)


def _picker_safe_json_loads(self: StockPickerService, value: Any) -> Any:
    if value in (None, "", b""):
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return None


def _picker_backtest_to_dict(self: StockPickerService, row: StockSelectionBacktest) -> Dict[str, Any]:
    return {
        "id": row.id,
        "candidate_id": row.candidate_id,
        "strategy_id": row.strategy_id,
        "code": row.code,
        "scan_date": row.scan_date.isoformat() if row.scan_date else None,
        "horizon_days": row.horizon_days,
        "status": row.status,
        "entry_date": row.entry_date.isoformat() if row.entry_date else None,
        "exit_date": row.exit_date.isoformat() if row.exit_date else None,
        "entry_price": row.entry_price,
        "exit_price": row.exit_price,
        "end_close": row.end_close,
        "max_high": row.max_high,
        "min_low": row.min_low,
        "return_pct": row.return_pct,
        "max_drawdown_pct": row.max_drawdown_pct,
        "outcome": row.outcome,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "evaluated_at": row.evaluated_at.isoformat() if row.evaluated_at else None,
    }


def _picker_candidate_to_dict(
    self: StockPickerService,
    row: StockSelectionCandidate,
    backtests: Optional[Sequence[StockSelectionBacktest]] = None,
) -> Dict[str, Any]:
    return {
        "id": row.id,
        "run_id": row.run_id,
        "code": row.code,
        "name": row.name,
        "strategy_id": row.strategy_id,
        "scan_date": row.scan_date.isoformat() if row.scan_date else None,
        "setup_type": row.setup_type,
        "score": float(row.score or 0.0),
        "rank": int(row.rank or 0),
        "selected": bool(row.selected),
        "operation_advice": row.operation_advice,
        "analysis_summary": row.analysis_summary,
        "action_plan_markdown": row.action_plan_markdown,
        "reasons": self._safe_json_loads(row.reason_json) or [],
        "metrics": self._safe_json_loads(row.indicator_snapshot_json) or {},
        "market_context": self._safe_json_loads(row.market_context_json) or {},
        "news_context": self._safe_json_loads(row.news_context_json),
        "llm_model": row.llm_model,
        "stop_loss": row.stop_loss,
        "take_profit": row.take_profit,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "backtests": [self._backtest_to_dict(item) for item in (backtests or [])],
    }


def _picker_run_to_dict(
    self: StockPickerService,
    row: StockSelectionRun,
    *,
    with_candidates: bool = False,
) -> Dict[str, Any]:
    payload = {
        "id": row.id,
        "run_id": row.id,
        "query_id": row.query_id,
        "strategy_id": row.strategy_id,
        "strategy_name": row.strategy_name,
        "scan_date": row.scan_date.isoformat() if row.scan_date else None,
        "status": row.status,
        "total_scanned": int(row.total_scanned or 0),
        "matched_count": int(row.matched_count or 0),
        "selected_count": int(row.selected_count or 0),
        "market_snapshot": self._safe_json_loads(row.market_snapshot_json) or {},
        "us_market_snapshot": self._safe_json_loads(row.us_market_snapshot_json) or {},
        "optimization": self._safe_json_loads(row.optimization_snapshot_json),
        "summary_markdown": row.summary_markdown,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
    }
    if with_candidates:
        payload["candidates"] = []
    return payload


def _picker_get_latest_optimization(
    self: StockPickerService,
    strategy_id: str,
) -> Optional[Dict[str, Any]]:
    with self.db.get_session() as session:
        rows = session.execute(
            select(StockSelectionOptimization)
            .where(StockSelectionOptimization.strategy_id == strategy_id)
            .order_by(desc(StockSelectionOptimization.created_at))
            .limit(12)
        ).scalars().all()

    for row in rows:
        params = self._safe_json_loads(row.params_json)
        metrics = self._safe_json_loads(row.metrics_json)
        if not isinstance(params, dict):
            continue
        if not isinstance(metrics, dict):
            metrics = {}
        if (row.status or "").strip().lower() != "completed":
            continue
        sample_count = int(self._to_float(metrics.get("sample_count"), 0.0))
        if sample_count < 6:
            continue
        return {
            "id": row.id,
            "strategy_id": row.strategy_id,
            "strategy_name": row.strategy_name,
            "lookback_days": int(row.lookback_days or 0),
            "selected_horizon_days": int(row.selected_horizon_days or 0),
            "status": row.status,
            "params": params,
            "metrics": metrics,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
    return None


def _picker_save_scan_run(
    self: StockPickerService,
    *,
    query_id: str,
    strategy: Dict[str, Any],
    scan_date: date,
    total_scanned: int,
    matched_count: int,
    selected: Sequence[Dict[str, Any]],
    market_snapshot: Dict[str, Any],
    us_snapshot: Dict[str, Any],
    optimization: Optional[Dict[str, Any]],
    status: str,
) -> int:
    summary_markdown = self._build_run_summary_markdown(
        strategy=strategy,
        scan_date=scan_date,
        selected=selected,
        market_snapshot=market_snapshot,
        us_snapshot=us_snapshot,
    )

    with self.db.session_scope() as session:
        run = StockSelectionRun(
            query_id=query_id,
            strategy_id=strategy["strategy_id"],
            strategy_name=strategy["name"],
            scan_date=scan_date,
            status=status,
            total_scanned=int(total_scanned or 0),
            matched_count=int(matched_count or 0),
            selected_count=len(selected),
            market_snapshot_json=json.dumps(market_snapshot or {}, ensure_ascii=False),
            us_market_snapshot_json=json.dumps(us_snapshot or {}, ensure_ascii=False),
            optimization_snapshot_json=json.dumps(optimization or {}, ensure_ascii=False),
            summary_markdown=summary_markdown,
            created_at=datetime.now(),
            completed_at=datetime.now() if status == "completed" else None,
        )
        session.add(run)
        session.flush()

        for candidate in selected:
            session.add(
                StockSelectionCandidate(
                    run_id=run.id,
                    code=str(candidate.get("code") or "").strip(),
                    name=str(candidate.get("name") or "").strip(),
                    strategy_id=str(candidate.get("strategy_id") or strategy["strategy_id"]),
                    scan_date=scan_date,
                    setup_type=str(candidate.get("setup_type") or "mixed"),
                    score=float(candidate.get("score") or 0.0),
                    rank=int(candidate.get("rank") or 0),
                    selected=True,
                    operation_advice=str(candidate.get("operation_advice") or ""),
                    analysis_summary=candidate.get("analysis_summary"),
                    action_plan_markdown=candidate.get("action_plan_markdown"),
                    reason_json=json.dumps(candidate.get("reasons") or [], ensure_ascii=False),
                    indicator_snapshot_json=json.dumps(candidate.get("metrics") or {}, ensure_ascii=False),
                    market_context_json=json.dumps(candidate.get("market_context") or {}, ensure_ascii=False),
                    news_context_json=json.dumps(candidate.get("news_context"), ensure_ascii=False)
                    if candidate.get("news_context") is not None
                    else None,
                    llm_model=candidate.get("llm_model"),
                    stop_loss=self._to_float(candidate.get("stop_loss")) if candidate.get("stop_loss") not in (None, "") else None,
                    take_profit=self._to_float(candidate.get("take_profit")) if candidate.get("take_profit") not in (None, "") else None,
                    created_at=datetime.now(),
                )
            )
        session.flush()
        return int(run.id)


def _picker_update_run_optimization(
    self: StockPickerService,
    *,
    run_id: int,
    optimization: Optional[Dict[str, Any]],
) -> None:
    with self.db.session_scope() as session:
        run = session.execute(
            select(StockSelectionRun).where(StockSelectionRun.id == run_id)
        ).scalar_one_or_none()
        if run is None:
            return
        run.optimization_snapshot_json = json.dumps(optimization or {}, ensure_ascii=False)


def _picker_load_forward_bars(
    self: StockPickerService,
    code: str,
    scan_date: date,
    max_days: int,
) -> pd.DataFrame:
    with self.db.get_session() as session:
        rows = session.execute(
            select(StockDaily)
            .where(
                and_(
                    StockDaily.code == code,
                    StockDaily.date > scan_date,
                )
            )
            .order_by(StockDaily.date.asc())
            .limit(max(max_days, 1))
        ).scalars().all()
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
    return pd.DataFrame(
        [
            {
                "date": row.date,
                "open": self._to_float(row.open),
                "high": self._to_float(row.high),
                "low": self._to_float(row.low),
                "close": self._to_float(row.close),
                "volume": self._to_float(row.volume),
                "amount": self._to_float(row.amount),
            }
            for row in rows
        ]
    )


def _picker_evaluate_candidate_horizon(
    self: StockPickerService,
    candidate: StockSelectionCandidate,
    bars: pd.DataFrame,
    horizon: int,
) -> Dict[str, Any]:
    if bars is None or bars.empty or len(bars) < horizon:
        return {"status": "pending"}

    window = bars.head(horizon).reset_index(drop=True)
    entry_bar = window.iloc[0]
    exit_bar = window.iloc[horizon - 1]
    entry_price = self._to_float(entry_bar.get("open")) or self._to_float(entry_bar.get("close"))
    exit_price = self._to_float(exit_bar.get("close"))
    if entry_price <= 0 or exit_price <= 0:
        return {"status": "pending"}

    max_high = self._to_float(window["high"].max())
    min_low = self._to_float(window["low"].min())
    return_pct = (exit_price - entry_price) / max(entry_price, 0.01) * 100.0
    max_drawdown_pct = (min_low - entry_price) / max(entry_price, 0.01) * 100.0
    if return_pct > 0.8:
        outcome = "win"
    elif return_pct < -0.8:
        outcome = "loss"
    else:
        outcome = "neutral"

    return {
        "status": "completed",
        "entry_date": entry_bar.get("date"),
        "exit_date": exit_bar.get("date"),
        "entry_price": round(entry_price, 2),
        "exit_price": round(exit_price, 2),
        "end_close": round(self._to_float(exit_bar.get("close")), 2),
        "max_high": round(max_high, 2),
        "min_low": round(min_low, 2),
        "return_pct": round(return_pct, 2),
        "max_drawdown_pct": round(max_drawdown_pct, 2),
        "outcome": outcome,
    }


def _picker_load_universe_from_local_db(self: StockPickerService) -> pd.DataFrame:
    with self.db.get_session() as session:
        rows = session.execute(
            select(StockDaily.code)
            .group_by(StockDaily.code)
            .order_by(StockDaily.code.asc())
        ).all()
    if not rows:
        return pd.DataFrame(columns=["code", "name"])
    return pd.DataFrame(
        [
            {
                "code": str(code or "").strip(),
                "name": str(code or "").strip(),
            }
            for (code,) in rows
            if str(code or "").strip()
        ]
    )


def _picker_resolve_scan_trade_date(self: StockPickerService) -> date:
    today = date.today()
    if self.tushare_fetcher.is_available():
        try:
            trade_dates = self.tushare_fetcher._get_trade_dates(end_date=today.strftime("%Y%m%d"))
            if trade_dates:
                return datetime.strptime(trade_dates[0], "%Y%m%d").date()
        except Exception as exc:
            logger.warning("Stock picker failed to resolve Tushare trade date: %s", exc)

    latest_db_date = self._get_latest_daily_date()
    if latest_db_date is not None and latest_db_date <= today:
        return latest_db_date

    if today.weekday() == 5:
        return today - timedelta(days=1)
    if today.weekday() == 6:
        return today - timedelta(days=2)
    return today


def _picker_is_reusable_run_payload(self: StockPickerService, payload: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(payload, dict):
        return False

    status = str(payload.get("status") or "").strip().lower()
    total_scanned = int(payload.get("total_scanned") or 0)
    selected_count = int(payload.get("selected_count") or 0)
    candidates = list(payload.get("candidates") or [])

    if status != "completed":
        return False
    if total_scanned < 1000:
        return False
    if selected_count <= 0:
        return True
    if not candidates:
        return False

    top_candidates = candidates[: min(len(candidates), _MAX_REPORT_CANDIDATES)]
    return all(
        not self._is_placeholder_action_plan(item.get("action_plan_markdown"))
        and str(item.get("action_plan_markdown") or "").strip()
        for item in top_candidates
    )


def _picker_run_scheduled_scan(self: StockPickerService) -> Dict[str, Any]:
    strategy_id = "mainboard_swing_master"
    scan_date = self._resolve_scan_trade_date()

    with self._schedule_lock:
        existing_run = self._find_latest_run_for_date(strategy_id=strategy_id, scan_date=scan_date)
        if existing_run is not None:
            self._repair_stuck_run(run_id=existing_run)
            payload = self.get_run_detail(existing_run)
            if self._is_reusable_run_payload(payload):
                return payload
            logger.warning(
                "Stock picker will rerun scheduled scan because existing run is incomplete or unusable: run_id=%s scan_date=%s",
                existing_run,
                scan_date,
            )

        return self.run_scan(
            strategy_id=strategy_id,
            scan_date=scan_date,
            max_candidates=_MAX_SELECTED_CANDIDATES,
            send_notification=True,
        )


def _picker_get_latest_market_snapshot(self: StockPickerService) -> Dict[str, Any]:
    with self.db.get_session() as session:
        row = session.execute(
            select(StockSelectionRun.market_snapshot_json, StockSelectionRun.completed_at)
            .where(StockSelectionRun.status == "completed")
            .order_by(desc(StockSelectionRun.scan_date), desc(StockSelectionRun.created_at))
            .limit(1)
        ).first()
    if not row:
        return {}
    snapshot = self._safe_json_loads(row[0]) or {}
    if row[1] is not None:
        snapshot["updated_at"] = row[1].isoformat()
    return snapshot


def _picker_get_market_sentiment(self: StockPickerService) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {}
    errors: List[str] = []
    for use_cache in (False, True):
        try:
            snapshot = self._build_market_snapshot(use_cache=use_cache) or {}
            if snapshot:
                break
        except Exception as exc:
            errors.append(str(exc))
    if not snapshot:
        snapshot = self._get_latest_market_snapshot()
    if not snapshot:
        if errors:
            raise RuntimeError(f"failed to load market sentiment: {' | '.join(errors[:2])}")
        raise RuntimeError("failed to load market sentiment")

    stats = snapshot.get("stats") or {}
    up_count = self._to_float(stats.get("up_count"))
    down_count = self._to_float(stats.get("down_count"))
    breadth = round(up_count / max(up_count + down_count, 1.0), 4) if (up_count + down_count) > 0 else None
    return {
        "market": "cn",
        "score": round(self._to_float(snapshot.get("score"), 50.0), 2),
        "regime": str(snapshot.get("regime") or "中性"),
        "summary": str(snapshot.get("summary") or "A股情绪数据暂不可用"),
        "breadth": breadth,
        "limit_up_count": self._to_float(stats.get("limit_up_count")) if stats else None,
        "limit_down_count": self._to_float(stats.get("limit_down_count")) if stats else None,
        "updated_at": snapshot.get("updated_at") or datetime.now().isoformat(),
    }


def _picker_build_notification_markdown(self: StockPickerService, payload: Dict[str, Any]) -> str:
    scan_date = payload.get("scan_date") or date.today().isoformat()
    strategy_name = payload.get("strategy_name") or payload.get("strategy_id") or "stock picker"
    market_snapshot = payload.get("market_snapshot") or {}
    us_snapshot = payload.get("us_market_snapshot") or {}
    candidates = (payload.get("candidates") or [])[:_MAX_REPORT_CANDIDATES]
    stage = str(payload.get("notification_stage") or "initial").strip().lower()
    stage_label = "增强版" if stage == "enhanced" else "模板版"
    stage_note = (
        "已补充新闻复核和 LLM 个股化操作手册。"
        if stage == "enhanced"
        else "先发模板版手册，新闻和 LLM 增强完成后会再次补发。"
    )

    lines = [
        f"# {scan_date} 收盘后选股 {stage_label}",
        "",
        f"- 策略：{strategy_name}",
        "- 范围：沪深主板（非 ST）",
        f"- 说明：{stage_note}",
        f"- A股情绪：{market_snapshot.get('summary') or market_snapshot.get('regime') or '中性'}",
        f"- 美股情绪：{us_snapshot.get('summary') or us_snapshot.get('mood') or '中性'}",
        f"- 扫描结果：入选 {payload.get('selected_count', 0)} / 命中 {payload.get('matched_count', 0)} / 扫描 {payload.get('total_scanned', 0)}",
        "",
    ]

    summary_markdown = payload.get("summary_markdown")
    if summary_markdown:
        lines.extend(["## 摘要", "", summary_markdown.strip(), ""])

    if not candidates:
        lines.extend(["## 候选股", "", "- 今日没有符合条件的标的。"])
        return "\n".join(lines)

    lines.extend(["## 候选股与次日操作手册", ""])
    for candidate in candidates:
        reasons = candidate.get("reasons") or []
        lines.append(
            f"### {candidate.get('rank', '-')}. {candidate.get('name') or candidate.get('code')} ({candidate.get('code')})"
        )
        lines.append(f"- 形态：{candidate.get('setup_type') or '未分类'}")
        lines.append(f"- 分数：{candidate.get('score')}")
        if candidate.get("analysis_summary"):
            lines.append(f"- 量化结论：{candidate.get('analysis_summary')}")
        if reasons:
            lines.append(f"- 触发原因：{'；'.join(str(item) for item in reasons[:6])}")
        if candidate.get("stop_loss") is not None or candidate.get("take_profit") is not None:
            lines.append(
                f"- 风控：止损 {candidate.get('stop_loss') or '-'}，止盈 {candidate.get('take_profit') or '-'}"
            )
        lines.extend(["", candidate.get("action_plan_markdown") or "暂无次日操作手册。", "", "---", ""])

    return "\n".join(lines).strip()


StockPickerService._format_price = staticmethod(_picker_format_price)
StockPickerService._strategy_catalog = _picker_strategy_catalog
StockPickerService.list_strategies = staticmethod(_picker_list_strategies)
StockPickerService._to_float = _picker_to_float
StockPickerService._score_intel_payload = _picker_score_intel_payload
StockPickerService._derive_trade_levels = _picker_derive_trade_levels
StockPickerService._build_stock_profile = _picker_build_stock_profile
StockPickerService._build_template_action_plan = _picker_build_template_action_plan
StockPickerService._build_action_plan = _picker_build_action_plan
StockPickerService._build_run_summary_markdown = _picker_build_run_summary_markdown
StockPickerService._send_scan_notification = _picker_send_scan_notification
StockPickerService._safe_json_loads = _picker_safe_json_loads
StockPickerService._backtest_to_dict = _picker_backtest_to_dict
StockPickerService._candidate_to_dict = _picker_candidate_to_dict
StockPickerService._run_to_dict = _picker_run_to_dict
StockPickerService._get_latest_optimization = _picker_get_latest_optimization
StockPickerService._save_scan_run = _picker_save_scan_run
StockPickerService._update_run_optimization = _picker_update_run_optimization
StockPickerService._load_forward_bars = _picker_load_forward_bars
StockPickerService._evaluate_candidate_horizon = _picker_evaluate_candidate_horizon
StockPickerService._load_universe_from_local_db = _picker_load_universe_from_local_db
StockPickerService._resolve_scan_trade_date = _picker_resolve_scan_trade_date
StockPickerService._is_reusable_run_payload = _picker_is_reusable_run_payload
StockPickerService.run_scheduled_scan = _picker_run_scheduled_scan
StockPickerService._get_latest_market_snapshot = _picker_get_latest_market_snapshot
StockPickerService.get_market_sentiment = _picker_get_market_sentiment
StockPickerService._build_notification_markdown = _picker_build_notification_markdown
