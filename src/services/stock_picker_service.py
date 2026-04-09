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
        with self.db.get_session() as session:
            rows = session.execute(
                select(StockSelectionRun)
                .order_by(desc(StockSelectionRun.scan_date), desc(StockSelectionRun.created_at))
                .limit(limit)
            ).scalars().all()
            return [self._run_to_dict(row, with_candidates=False) for row in rows]

    def get_run_detail(self, run_id: int) -> Optional[Dict[str, Any]]:
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

            backtest_stats = service.backfill_backtests(strategy_id=strategy["strategy_id"])
            optimization = service.optimize_strategy(
                strategy_id=strategy["strategy_id"],
                selected_horizon_days=5,
            )
            service._update_run_optimization(run_id=run_id, optimization=optimization)
            service._update_run_status(run_id=run_id, status="completed", completed=True)

            payload = service.get_run_detail(run_id)
            if payload is not None:
                payload["backtest_stats"] = backtest_stats
                payload["optimization"] = optimization
                if send_notification:
                    service._send_scan_notification(payload)
        except Exception as exc:
            logger.exception("Stock picker background enrichment failed for run %s: %s", run_id, exc)
            service._update_run_status(run_id=run_id, status="failed", completed=False)

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
            ).scalar_one_or_none()
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

    def _build_action_plan(
        self,
        *,
        candidate: Dict[str, Any],
        strategy: Dict[str, Any],
        market_snapshot: Dict[str, Any],
        us_snapshot: Dict[str, Any],
        review_payload: Optional[Dict[str, Any]],
    ) -> tuple[str, Optional[str]]:
        review_text = review_payload.get("news_context") if isinstance(review_payload, dict) else None
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
            "你是 A 股波段交易助理。请根据下面的收盘后选股结果，为第二个交易日生成一份简洁、可执行的操作手册。\n"
            "必须包含：入场前提、平开/高开/低开三种预案、放弃条件、5到10天持有期间的跟踪重点、止损止盈。\n"
            "不要夸张，不要编造未提供的数据。\n\n"
            f"策略：{strategy['name']}\n"
            f"股票：{candidate['name']} ({candidate['code']})\n"
            f"量化结论：{candidate['analysis_summary']}\n"
            f"形态类型：{candidate['setup_type']}\n"
            f"分数：{candidate['score']}\n"
            f"触发原因：{'；'.join(candidate['reasons'])}\n"
            f"A股情绪：{market_snapshot.get('summary')}\n"
            f"美股情绪：{us_snapshot.get('summary')}\n"
            f"止损参考：{candidate.get('stop_loss')}\n"
            f"止盈参考：{candidate.get('take_profit')}\n"
            f"消息面摘要：{review_text or '暂无额外消息，按中性处理。'}"
        )
        generated = self.analyzer.generate_text(prompt, max_tokens=1200, temperature=0.35)
        if not generated:
            return fallback, None
        return generated, getattr(self.config, "litellm_model", None)

    @staticmethod
    def _build_template_action_plan(
        *,
        candidate: Dict[str, Any],
        strategy: Dict[str, Any],
        market_snapshot: Dict[str, Any],
        us_snapshot: Dict[str, Any],
        review_text: Optional[str],
    ) -> str:
        return (
            f"## {candidate['name']} {strategy['name']} 次日操作手册\n\n"
            f"- 核心判断：{candidate['analysis_summary']}\n"
            f"- 市场环境：{market_snapshot.get('summary')}；{us_snapshot.get('summary')}\n"
            "- 高开方案：高开过大（>4%）不追；若高开 0% 到 2.5% 且前 30 分钟承接稳定，再观察介入。\n"
            "- 平开/小低开方案：优先看 5 日线或 20 日线附近承接，缩量回踩优于放量下杀。\n"
            "- 放弃条件：跌破关键均线、板块龙头转弱、消息面出现明显利空。\n"
            "- 持仓管理：计划持有 5 到 10 天；若放量跌破 5 日线先减仓，跌破止损位执行离场。\n"
            f"- 止损位：{candidate.get('stop_loss')}；止盈位：{candidate.get('take_profit')}\n"
            f"- 消息面备注：{review_text or '暂无额外负面消息，保持中性。'}\n"
        )

    @staticmethod
    def _build_run_summary_markdown(
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
            f"- A股情绪：{market_snapshot.get('summary')}",
            f"- 美股情绪：{us_snapshot.get('summary')}",
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

    def _build_notification_markdown(self, payload: Dict[str, Any]) -> str:
        scan_date = payload.get("scan_date") or date.today().isoformat()
        strategy_name = payload.get("strategy_name") or payload.get("strategy_id") or "stock picker"
        market_snapshot = payload.get("market_snapshot") or {}
        us_snapshot = payload.get("us_market_snapshot") or {}
        candidates = (payload.get("candidates") or [])[:_MAX_REPORT_CANDIDATES]

        lines = [
            f"# {scan_date} 收盘后选股",
            "",
            f"- 策略：{strategy_name}",
            "- 范围：沪深主板（非 ST）",
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

    def _score_intel_payload(self, intel_results: Dict[str, SearchResponse]) -> float:
        positive_keywords = ("订单", "中标", "预增", "增长", "回购", "增持", "景气", "评级上调", "新产品")
        negative_keywords = ("减持", "立案", "处罚", "亏损", "诉讼", "风险", "爆雷", "问询", "质押", "违约")
        score = 50.0
        for response in intel_results.values():
            if not response.success:
                continue
            for item in response.results[:3]:
                text = f"{item.title} {item.snippet}"
                score += sum(2.5 for keyword in positive_keywords if keyword in text)
                score -= sum(4.0 for keyword in negative_keywords if keyword in text)
        return max(0.0, min(score, 100.0))

    def _build_action_plan(
        self,
        *,
        candidate: Dict[str, Any],
        strategy: Dict[str, Any],
        market_snapshot: Dict[str, Any],
        us_snapshot: Dict[str, Any],
        review_payload: Optional[Dict[str, Any]],
    ) -> tuple[str, Optional[str]]:
        review_text = review_payload.get("news_context") if isinstance(review_payload, dict) else None
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
            "你是一名A股波段交易助手。请基于以下收盘后选股结果，为第二个交易日生成一份简洁、可执行的操作手册。"
            "必须包含：入场前提、平开/高开/低开三种方案、放弃条件、持仓5到10天的跟踪重点、止损止盈。"
            "不要夸张，不要编造未给出的数据。\n\n"
            f"策略：{strategy['name']}\n"
            f"股票：{candidate['name']} ({candidate['code']})\n"
            f"量化结论：{candidate['analysis_summary']}\n"
            f"形态类型：{candidate['setup_type']}\n"
            f"分数：{candidate['score']}\n"
            f"触发原因：{'；'.join(candidate['reasons'])}\n"
            f"A股情绪：{market_snapshot.get('summary')}\n"
            f"美股情绪：{us_snapshot.get('summary')}\n"
            f"止损参考：{candidate.get('stop_loss')}\n"
            f"止盈参考：{candidate.get('take_profit')}\n"
            f"消息面摘要：{review_text or '暂无额外消息，按中性处理。'}"
        )
        generated = self.analyzer.generate_text(prompt, max_tokens=1200, temperature=0.35)
        if not generated:
            return fallback, None
        return generated, getattr(self.config, "litellm_model", None)

    @staticmethod
    def _build_template_action_plan(
        *,
        candidate: Dict[str, Any],
        strategy: Dict[str, Any],
        market_snapshot: Dict[str, Any],
        us_snapshot: Dict[str, Any],
        review_text: Optional[str],
    ) -> str:
        return (
            f"## {candidate['name']} {strategy['name']} 次日操作手册\n\n"
            f"- 核心判断：{candidate['analysis_summary']}\n"
            f"- 市场环境：{market_snapshot.get('summary')}；{us_snapshot.get('summary')}\n"
            f"- 高开方案：高开过大（>4%）不追，高开 0%~2.5% 且 30 分钟内承接稳定时再观察。\n"
            f"- 平开/小低开方案：优先看 5 日线或 20 日线附近承接，缩量回踩优于放量下杀。\n"
            f"- 放弃条件：跌破关键均线、板块龙头转弱、消息面出现明显利空。\n"
            f"- 持仓管理：计划持有 5 到 10 天，若放量跌破 5 日线先减仓，跌破止损位执行离场。\n"
            f"- 止损位：{candidate.get('stop_loss')}；止盈位：{candidate.get('take_profit')}\n"
            f"- 消息面备注：{review_text or '暂无额外负面消息，保持中性。'}\n"
        )

    def _save_scan_run(
        self,
        *,
        query_id: str,
        strategy: Dict[str, Any],
        scan_date: date,
        total_scanned: int,
        matched_count: int,
        selected: List[Dict[str, Any]],
        market_snapshot: Dict[str, Any],
        us_snapshot: Dict[str, Any],
        optimization: Optional[Dict[str, Any]],
        status: str = "completed",
    ) -> int:
        summary = self._build_run_summary_markdown(strategy, scan_date, selected, market_snapshot, us_snapshot)
        with self.db.session_scope() as session:
            run = StockSelectionRun(
                query_id=query_id,
                strategy_id=strategy["strategy_id"],
                strategy_name=strategy["name"],
                scan_date=scan_date,
                status=status,
                total_scanned=total_scanned,
                matched_count=matched_count,
                selected_count=len(selected),
                market_snapshot_json=json.dumps(market_snapshot, ensure_ascii=False),
                us_market_snapshot_json=json.dumps(us_snapshot, ensure_ascii=False),
                optimization_snapshot_json=json.dumps(optimization, ensure_ascii=False) if optimization else None,
                summary_markdown=summary,
                created_at=datetime.now(),
                completed_at=datetime.now() if status == "completed" else None,
            )
            session.add(run)
            session.flush()

            for candidate in selected:
                session.add(
                    StockSelectionCandidate(
                        run_id=run.id,
                        code=candidate["code"],
                        name=candidate["name"],
                        strategy_id=candidate["strategy_id"],
                        scan_date=scan_date,
                        setup_type=candidate["setup_type"],
                        score=candidate["score"],
                        rank=candidate["rank"],
                        selected=True,
                        operation_advice=candidate["operation_advice"],
                        analysis_summary=candidate["analysis_summary"],
                        action_plan_markdown=candidate["action_plan_markdown"],
                        reason_json=json.dumps(candidate["reasons"], ensure_ascii=False),
                        indicator_snapshot_json=json.dumps(candidate["metrics"], ensure_ascii=False),
                        market_context_json=json.dumps(candidate["market_context"], ensure_ascii=False),
                        news_context_json=json.dumps(candidate.get("news_context"), ensure_ascii=False) if candidate.get("news_context") else None,
                        llm_model=candidate.get("llm_model"),
                        stop_loss=candidate.get("stop_loss"),
                        take_profit=candidate.get("take_profit"),
                        created_at=datetime.now(),
                    )
                )
            session.flush()
            return int(run.id)

    def _update_run_optimization(self, *, run_id: int, optimization: Dict[str, Any]) -> None:
        with self.db.session_scope() as session:
            run = session.execute(
                select(StockSelectionRun).where(StockSelectionRun.id == run_id)
            ).scalar_one_or_none()
            if run is None:
                return
            run.optimization_snapshot_json = json.dumps(optimization, ensure_ascii=False)

    @staticmethod
    def _build_run_summary_markdown(
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
            f"- A股情绪：{market_snapshot.get('summary')}",
            f"- 美股情绪：{us_snapshot.get('summary')}",
            "",
            "## 候选股",
        ]
        if not selected:
            lines.append("- 无符合条件的标的")
            return "\n".join(lines)
        for item in selected:
            lines.append(
                f"- {item['rank']}. {item['name']}({item['code']}) | 分数 {item['score']} | {item['setup_type']} | {item['analysis_summary']}"
            )
        return "\n".join(lines)

    def _load_forward_bars(self, code: str, scan_date: date, limit: int) -> List[StockDaily]:
        with self.db.get_session() as session:
            rows = session.execute(
                select(StockDaily)
                .where(and_(StockDaily.code == code, StockDaily.date > scan_date))
                .order_by(StockDaily.date.asc())
                .limit(limit)
            ).scalars().all()
            return list(rows)

    def _evaluate_candidate_horizon(
        self,
        candidate: StockSelectionCandidate,
        bars: Sequence[StockDaily],
        horizon_days: int,
    ) -> Dict[str, Any]:
        if len(bars) < horizon_days:
            return {"status": "pending"}
        entry_bar = bars[0]
        window = list(bars[:horizon_days])
        entry_price = self._to_float(entry_bar.open or entry_bar.close)
        if entry_price <= 0:
            return {"status": "pending"}
        end_bar = window[-1]
        max_high = max(self._to_float(bar.high) for bar in window if bar.high is not None)
        min_low = min(self._to_float(bar.low) for bar in window if bar.low is not None)
        end_close = self._to_float(end_bar.close)
        return_pct = (end_close - entry_price) / entry_price * 100
        max_drawdown_pct = (min_low - entry_price) / entry_price * 100
        if return_pct > 3:
            outcome = "win"
        elif return_pct < -2:
            outcome = "loss"
        else:
            outcome = "neutral"
        return {
            "status": "completed",
            "entry_date": entry_bar.date,
            "exit_date": end_bar.date,
            "entry_price": round(entry_price, 3),
            "exit_price": round(end_close, 3),
            "end_close": round(end_close, 3),
            "max_high": round(max_high, 3),
            "min_low": round(min_low, 3),
            "return_pct": round(return_pct, 2),
            "max_drawdown_pct": round(max_drawdown_pct, 2),
            "outcome": outcome,
        }

    def _get_latest_optimization(self, strategy_id: str) -> Optional[Dict[str, Any]]:
        with self.db.get_session() as session:
            row = session.execute(
                select(StockSelectionOptimization)
                .where(StockSelectionOptimization.strategy_id == strategy_id)
                .order_by(desc(StockSelectionOptimization.created_at))
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            metrics = self._safe_json_loads(row.metrics_json)
            sample_count = 0
            if isinstance(metrics, dict):
                sample_count = int(metrics.get("sample_count") or 0)
            if row.status != "completed" or sample_count < 6:
                return None
            return {
                "strategy_id": row.strategy_id,
                "strategy_name": row.strategy_name,
                "lookback_days": row.lookback_days,
                "selected_horizon_days": row.selected_horizon_days,
                "status": row.status,
                "params": self._safe_json_loads(row.params_json),
                "metrics": metrics,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            }

    def _send_scan_notification(self, payload: Dict[str, Any]) -> None:
        try:
            NotificationService().send(self._build_notification_markdown(payload))
        except Exception as exc:
            logger.warning("Stock picker notification failed: %s", exc)

    def _build_notification_markdown(self, payload: Dict[str, Any]) -> str:
        scan_date = payload.get("scan_date") or date.today().isoformat()
        strategy_name = payload.get("strategy_name") or payload.get("strategy_id") or "stock picker"
        market_snapshot = payload.get("market_snapshot") or {}
        us_snapshot = payload.get("us_market_snapshot") or {}
        candidates = (payload.get("candidates") or [])[:_MAX_REPORT_CANDIDATES]

        lines = [
            f"# {scan_date} 收盘后选股",
            "",
            f"- 策略：{strategy_name}",
            f"- A股情绪：{market_snapshot.get('summary') or market_snapshot.get('regime') or '中性'}",
            f"- 美股情绪：{us_snapshot.get('summary') or us_snapshot.get('mood') or '中性'}",
            f"- 扫描结果：入选 {payload.get('selected_count', 0)} / 命中 {payload.get('matched_count', 0)} / 扫描 {payload.get('total_scanned', 0)}",
            "",
        ]

        summary_markdown = payload.get("summary_markdown")
        if summary_markdown:
            lines.extend(["## 摘要", "", summary_markdown.strip(), ""])

        if not candidates:
            lines.extend(["## 候选股", "", "- 今日无符合条件的标的。"])
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

    def _score_intel_payload(self, intel_results: Dict[str, SearchResponse]) -> float:
        positive_keywords = ("订单", "中标", "预增", "增长", "回购", "增持", "景气", "评级上调", "新产品")
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

    def _build_action_plan(
        self,
        *,
        candidate: Dict[str, Any],
        strategy: Dict[str, Any],
        market_snapshot: Dict[str, Any],
        us_snapshot: Dict[str, Any],
        review_payload: Optional[Dict[str, Any]],
    ) -> tuple[str, Optional[str]]:
        review_text = review_payload.get("news_context") if isinstance(review_payload, dict) else None
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
            "你是一名 A 股波段交易助手。请基于以下收盘后选股结果，为第二个交易日生成一份简洁、可执行的操作手册。"
            "必须包含：入场前提、平开/高开/低开三种方案、放弃条件、持有 5 到 10 天的跟踪重点、止损止盈。"
            "不要夸张，不要编造未给出的数据。\n\n"
            f"策略：{strategy['name']}\n"
            f"股票：{candidate['name']} ({candidate['code']})\n"
            f"量化结论：{candidate['analysis_summary']}\n"
            f"形态类型：{candidate['setup_type']}\n"
            f"分数：{candidate['score']}\n"
            f"触发原因：{'，'.join(candidate['reasons'])}\n"
            f"A股情绪：{market_snapshot.get('summary')}\n"
            f"美股情绪：{us_snapshot.get('summary')}\n"
            f"止损参考：{candidate.get('stop_loss')}\n"
            f"止盈参考：{candidate.get('take_profit')}\n"
            f"消息面摘要：{review_text or '暂无额外消息，按中性处理。'}"
        )
        generated = self.analyzer.generate_text(prompt, max_tokens=1200, temperature=0.35)
        if not generated:
            return fallback, None
        return generated, getattr(self.config, "litellm_model", None)

    @staticmethod
    def _build_template_action_plan(
        *,
        candidate: Dict[str, Any],
        strategy: Dict[str, Any],
        market_snapshot: Dict[str, Any],
        us_snapshot: Dict[str, Any],
        review_text: Optional[str],
    ) -> str:
        return (
            f"## {candidate['name']} {strategy['name']} 次日操作手册\n\n"
            f"- 核心判断：{candidate['analysis_summary']}\n"
            f"- 市场环境：{market_snapshot.get('summary')}；{us_snapshot.get('summary')}\n"
            f"- 高开方案：高开过大（>4%）不追，高开 0% 到 2.5% 且前 30 分钟承接稳定时再观察。\n"
            f"- 平开/小低开方案：优先看 5 日线或 20 日线附近承接，缩量回踩优于放量下杀。\n"
            f"- 放弃条件：跌破关键均线、板块龙头转弱、消息面出现明显利空。\n"
            f"- 持仓管理：计划持有 5 到 10 天，若放量跌破 5 日线先减仓，跌破止损位执行离场。\n"
            f"- 止损位：{candidate.get('stop_loss')}；止盈位：{candidate.get('take_profit')}\n"
            f"- 消息面备注：{review_text or '暂无额外负面消息，保持中性。'}\n"
        )

    @staticmethod
    def _build_run_summary_markdown(
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
            f"- A股情绪：{market_snapshot.get('summary')}",
            f"- 美股情绪：{us_snapshot.get('summary')}",
            "",
            "## 候选股",
        ]
        if not selected:
            lines.append("- 无符合条件的标的")
            return "\n".join(lines)
        for item in selected:
            lines.append(
                f"- {item['rank']}. {item['name']}({item['code']}) | 分数 {item['score']} | {item['setup_type']} | {item['analysis_summary']}"
            )
        return "\n".join(lines)

    def _build_notification_markdown(self, payload: Dict[str, Any]) -> str:
        scan_date = payload.get("scan_date") or date.today().isoformat()
        strategy_name = payload.get("strategy_name") or payload.get("strategy_id") or "stock picker"
        market_snapshot = payload.get("market_snapshot") or {}
        us_snapshot = payload.get("us_market_snapshot") or {}
        candidates = payload.get("candidates") or []

        lines = [
            f"# {scan_date} 收盘后选股",
            "",
            f"- 策略：{strategy_name}",
            f"- A股情绪：{market_snapshot.get('summary') or market_snapshot.get('regime') or '中性'}",
            f"- 美股情绪：{us_snapshot.get('summary') or us_snapshot.get('mood') or '中性'}",
            f"- 扫描结果：入选 {payload.get('selected_count', 0)} / 命中 {payload.get('matched_count', 0)} / 扫描 {payload.get('total_scanned', 0)}",
            "",
        ]

        summary_markdown = payload.get("summary_markdown")
        if summary_markdown:
            lines.extend(["## 摘要", "", summary_markdown.strip(), ""])

        if not candidates:
            lines.extend(["## 候选股", "", "- 今日无符合条件的标的。"])
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
                lines.append(f"- 触发原因：{'，'.join(str(item) for item in reasons[:6])}")
            if candidate.get("stop_loss") is not None or candidate.get("take_profit") is not None:
                lines.append(
                    f"- 风控：止损 {candidate.get('stop_loss') or '-'}，止盈 {candidate.get('take_profit') or '-'}"
                )
            lines.extend(["", candidate.get("action_plan_markdown") or "暂无次日操作手册。", "", "---", ""])

        return "\n".join(lines).strip()

    def _run_to_dict(self, run: StockSelectionRun, *, with_candidates: bool) -> Dict[str, Any]:
        payload = {
            "id": run.id,
            "query_id": run.query_id,
            "strategy_id": run.strategy_id,
            "strategy_name": run.strategy_name,
            "scan_date": run.scan_date.isoformat() if run.scan_date else None,
            "status": run.status,
            "total_scanned": run.total_scanned,
            "matched_count": run.matched_count,
            "selected_count": run.selected_count,
            "market_snapshot": self._safe_json_loads(run.market_snapshot_json),
            "us_market_snapshot": self._safe_json_loads(run.us_market_snapshot_json),
            "optimization": self._safe_json_loads(run.optimization_snapshot_json),
            "summary_markdown": run.summary_markdown,
            "created_at": run.created_at.isoformat() if run.created_at else None,
            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        }
        if with_candidates:
            payload["candidates"] = []
        return payload

    def _candidate_to_dict(
        self,
        candidate: StockSelectionCandidate,
        backtests: Sequence[StockSelectionBacktest],
    ) -> Dict[str, Any]:
        return {
            "id": candidate.id,
            "run_id": candidate.run_id,
            "code": candidate.code,
            "name": candidate.name,
            "strategy_id": candidate.strategy_id,
            "scan_date": candidate.scan_date.isoformat() if candidate.scan_date else None,
            "setup_type": candidate.setup_type,
            "score": candidate.score,
            "rank": candidate.rank,
            "selected": candidate.selected,
            "operation_advice": candidate.operation_advice,
            "analysis_summary": candidate.analysis_summary,
            "action_plan_markdown": candidate.action_plan_markdown,
            "reasons": self._safe_json_loads(candidate.reason_json) or [],
            "metrics": self._safe_json_loads(candidate.indicator_snapshot_json) or {},
            "market_context": self._safe_json_loads(candidate.market_context_json) or {},
            "news_context": self._safe_json_loads(candidate.news_context_json),
            "llm_model": candidate.llm_model,
            "stop_loss": candidate.stop_loss,
            "take_profit": candidate.take_profit,
            "backtests": [self._backtest_to_dict(item) for item in backtests],
            "created_at": candidate.created_at.isoformat() if candidate.created_at else None,
        }

    @staticmethod
    def _backtest_to_dict(backtest: StockSelectionBacktest) -> Dict[str, Any]:
        return {
            "id": backtest.id,
            "candidate_id": backtest.candidate_id,
            "strategy_id": backtest.strategy_id,
            "code": backtest.code,
            "scan_date": backtest.scan_date.isoformat() if backtest.scan_date else None,
            "horizon_days": backtest.horizon_days,
            "status": backtest.status,
            "entry_date": backtest.entry_date.isoformat() if backtest.entry_date else None,
            "exit_date": backtest.exit_date.isoformat() if backtest.exit_date else None,
            "entry_price": backtest.entry_price,
            "exit_price": backtest.exit_price,
            "end_close": backtest.end_close,
            "max_high": backtest.max_high,
            "min_low": backtest.min_low,
            "return_pct": backtest.return_pct,
            "max_drawdown_pct": backtest.max_drawdown_pct,
            "outcome": backtest.outcome,
        }

    @staticmethod
    def _safe_json_loads(raw: Optional[str]) -> Any:
        if not raw:
            return None
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_float(value: Any) -> float:
        if value is None:
            return 0.0
        if isinstance(value, float):
            if math.isnan(value):
                return 0.0
            return value
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    def _strategy_catalog(cls) -> List[Dict[str, Any]]:
        catalog: List[Dict[str, Any]] = [
            {
                "strategy_id": "mainboard_swing_master",
                "name": "主力波段双模",
                "description": "收盘后筛选主板非 ST 标的，兼顾突破启动和趋势回踩，叠加市场情绪与消息面审核。",
                "skill_id": "swing_after_close_picker",
                "category": "swing",
                "params": dict(_PICKER_STRATEGY_PRESETS["mainboard_swing_master"]["params"]),
                "scan_family": _PICKER_STRATEGY_PRESETS["mainboard_swing_master"]["scan_family"],
                "priority": _PICKER_STRATEGY_PRESETS["mainboard_swing_master"]["priority"],
            }
        ]

        try:
            loaded_skills = load_skills_from_directory(_BUILTIN_STRATEGY_DIR)
        except Exception as exc:
            logger.warning("Failed to load built-in picker strategies: %s", exc)
            loaded_skills = []

        for skill in loaded_skills:
            if not getattr(skill, "user_invocable", True):
                continue
            if skill.name == "swing_after_close_picker":
                continue
            preset = _PICKER_STRATEGY_PRESETS.get(skill.name)
            default_params = {
                "min_score_threshold": 70.0,
                "volume_spike_multiplier": 1.5,
                "max_ma20_distance_pct": 4.0,
            }
            catalog.append(
                {
                    "strategy_id": skill.name,
                    "name": skill.display_name,
                    "description": skill.description,
                    "skill_id": skill.name,
                    "category": skill.category,
                    "params": dict((preset or {}).get("params", default_params)),
                    "scan_family": (preset or {}).get("scan_family", "mainboard_swing_master"),
                    "priority": int((preset or {}).get("priority", 999)),
                }
            )

        catalog.sort(key=lambda item: (int(item.get("priority", 999)), str(item.get("name", ""))))
        return catalog

    @classmethod
    def list_strategies(cls) -> List[Dict[str, Any]]:
        return [
            {
                "strategy_id": item["strategy_id"],
                "name": item["name"],
                "description": item["description"],
                "skill_id": item["skill_id"],
                "category": item["category"],
                "params": dict(item["params"]),
            }
            for item in cls._strategy_catalog()
        ]

    @staticmethod
    def _resolve_scan_family(strategy_id: str) -> str:
        if strategy_id == "swing_after_close_picker":
            return "mainboard_swing_master"
        preset = _PICKER_STRATEGY_PRESETS.get(strategy_id)
        if preset:
            return str(preset.get("scan_family", "mainboard_swing_master"))
        return "mainboard_swing_master"

    def get_market_sentiment(self) -> Dict[str, Any]:
        latest_run = self.list_runs(limit=1)
        latest_snapshot = latest_run[0].get("market_snapshot") if latest_run else None
        try:
            snapshot = self._build_market_snapshot(use_cache=False)
            updated_at = datetime.now().isoformat()
        except Exception as exc:
            logger.warning("Stock picker market sentiment build failed, fallback to latest run: %s", exc)
            snapshot = latest_snapshot or {"score": 50.0, "regime": "中性", "summary": "暂无最新大盘情绪数据。"}
            updated_at = latest_run[0].get("completed_at") if latest_run else datetime.now().isoformat()

        stats = snapshot.get("stats") if isinstance(snapshot.get("stats"), dict) else {}
        up_count = self._to_float(stats.get("up_count"))
        down_count = self._to_float(stats.get("down_count"))
        breadth = None
        if up_count + down_count > 0:
            breadth = round(up_count / max(up_count + down_count, 1.0), 4)

        return {
            "market": "沪A大盘情绪",
            "score": round(self._to_float(snapshot.get("score", 50.0)), 2),
            "regime": str(snapshot.get("regime") or "中性"),
            "summary": str(snapshot.get("summary") or "暂无最新大盘情绪数据。"),
            "breadth": snapshot.get("breadth", breadth),
            "limit_up_count": snapshot.get("limit_up_count", stats.get("limit_up_count")),
            "limit_down_count": snapshot.get("limit_down_count", stats.get("limit_down_count")),
            "updated_at": updated_at or datetime.now().isoformat(),
        }

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
            return StrategySignal(False, 0.0, "insufficient", "观察", "样本不足", [], None, None, {})

        strategy_family = self._resolve_scan_family(strategy_id)
        today = bars.iloc[-1]
        prev = bars.iloc[-2]
        recent8 = bars.tail(8)

        if any(pd.isna(today.get(col)) for col in ("ma5", "ma10", "ma20", "ma30", "ma60")):
            return StrategySignal(False, 0.0, "insufficient", "观察", "均线样本不足", [], None, None, {})

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
        volume_spike_factor = float(
            (recent8["volume"] / recent8["vol_ma20"].replace(0, pd.NA)).max(skipna=True) or 0.0
        )
        ma5_cross_ma20 = bool(prev["ma5"] <= prev["ma20"] and today["ma5"] > today["ma20"])
        ma5_cross_recent = bool(
            (bars["ma5"].shift(1) <= bars["ma20"].shift(1)).tail(2).any()
            and (bars["ma5"] > bars["ma20"]).tail(2).any()
        )
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
        breakout_near_high = bool(
            close_price >= self._to_float(today["max_high_20_prev"]) * 0.985
            if not pd.isna(today["max_high_20_prev"])
            else False
        )
        macd_bull = bool(today["macd_dif"] > today["macd_dea"] and today["macd_hist"] > -0.02)
        kdj_bull = bool(today["kdj_j"] >= today["kdj_k"])
        strong_body = bool(self._to_float(today["pct_chg"]) >= 2.0)

        reasons: List[str] = []
        score = 0.0
        passed = False
        setup_type = "mixed"

        if strategy_family in {"mainboard_swing_master", "ma_golden_cross"}:
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
                reasons.append("5 日线近期上穿 20 日线，20 日与 60 日线同步拐头向上")
            if ma5_cross_ma20:
                score += 8
                reasons.append("当日形成明确金叉")
            if strong_body:
                score += 8
                reasons.append("收盘中阳确认启动")
            if breakout_near_high:
                score += 10
                reasons.append("接近近期突破位收盘")
            if macd_bull:
                score += 8
                reasons.append("MACD 多头")
            if kdj_bull:
                score += 6
            passed = breakout_mode

        if strategy_family in {"mainboard_swing_master", "shrink_pullback"}:
            pullback_mode = (
                trend_stack_up
                and touch_ma5
                and ma60_up
                and volume_spike_factor >= float(params.get("volume_spike_multiplier", 1.5))
            )
            if pullback_mode:
                setup_type = "pullback" if strategy_family != "mainboard_swing_master" or not passed else "mixed"
                score += 34
                reasons.append("5/10/20/30 日线多头向上，K 线回踩 5 日线后仍守住趋势")
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

        if strategy_family == "volume_breakout":
            breakout_mode = (
                breakout_near_high
                and volume_spike_factor >= float(params.get("volume_spike_multiplier", 2.0))
                and ma20_up
                and ma60_up
            )
            if breakout_mode:
                setup_type = "breakout"
                score += 42
                reasons.append("放量突破近 20 日高点")
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
        market_score = float(market_snapshot.get("score", 50) or 50)
        if strategy_family == "mainboard_swing_master":
            if market_score >= 80:
                min_score_threshold -= 4.0
            elif market_score >= 70:
                min_score_threshold -= 2.0
        passed = passed and score >= min_score_threshold

        metrics = {
            "distance_to_ma20_pct": round(distance_to_ma20_pct, 2),
            "distance_to_ma5_pct": round(distance_to_ma5_pct, 2),
            "volume_spike_factor": round(volume_spike_factor, 2),
            "trend_stack_up": trend_stack_up,
            "ma20_up": ma20_up,
            "ma60_up": ma60_up,
            "macd_bull": macd_bull,
            "kdj_bull": kdj_bull,
            "scan_family": strategy_family,
            "effective_min_score_threshold": round(min_score_threshold, 2),
        }

        if not passed:
            return StrategySignal(
                passed=False,
                score=score,
                setup_type=setup_type,
                operation_advice="观察",
                analysis_summary=f"{name} 尚未达到 {strategy_id} 的触发阈值。",
                reasons=reasons,
                stop_loss=None,
                take_profit=None,
                metrics=metrics,
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
            metrics=metrics,
        )

    def _score_intel_payload(self, intel_results: Dict[str, SearchResponse]) -> float:
        positive_keywords = ("订单", "中标", "预增", "增长", "回购", "增持", "景气", "评级上调", "新产品")
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

    def _build_action_plan(
        self,
        *,
        candidate: Dict[str, Any],
        strategy: Dict[str, Any],
        market_snapshot: Dict[str, Any],
        us_snapshot: Dict[str, Any],
        review_payload: Optional[Dict[str, Any]],
    ) -> tuple[str, Optional[str]]:
        review_text = review_payload.get("news_context") if isinstance(review_payload, dict) else None
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
            "你是一名 A 股波段交易助手。请基于以下收盘后选股结果，为第二个交易日生成一份简洁、可执行的操作手册。"
            "必须包含：入场前提、平开/高开/低开三种方案、放弃条件、持有 5 到 10 天的跟踪重点、止损止盈。"
            "不要夸张，不要编造未给出的数据。\n\n"
            f"策略：{strategy['name']}\n"
            f"股票：{candidate['name']} ({candidate['code']})\n"
            f"量化结论：{candidate['analysis_summary']}\n"
            f"形态类型：{candidate['setup_type']}\n"
            f"分数：{candidate['score']}\n"
            f"触发原因：{'，'.join(candidate['reasons'])}\n"
            f"A股情绪：{market_snapshot.get('summary')}\n"
            f"美股情绪：{us_snapshot.get('summary')}\n"
            f"止损参考：{candidate.get('stop_loss')}\n"
            f"止盈参考：{candidate.get('take_profit')}\n"
            f"消息面摘要：{review_text or '暂无额外消息，按中性处理。'}"
        )
        generated = self.analyzer.generate_text(prompt, max_tokens=1200, temperature=0.35)
        if not generated:
            return fallback, None
        return generated, getattr(self.config, "litellm_model", None)

    @staticmethod
    def _build_template_action_plan(
        *,
        candidate: Dict[str, Any],
        strategy: Dict[str, Any],
        market_snapshot: Dict[str, Any],
        us_snapshot: Dict[str, Any],
        review_text: Optional[str],
    ) -> str:
        return (
            f"## {candidate['name']} {strategy['name']} 次日操作手册\n\n"
            f"- 核心判断：{candidate['analysis_summary']}\n"
            f"- 市场环境：{market_snapshot.get('summary')}；{us_snapshot.get('summary')}\n"
            f"- 高开方案：高开过大（>4%）不追，高开 0% 到 2.5% 且前 30 分钟承接稳定时再观察。\n"
            f"- 平开/小低开方案：优先看 5 日线或 20 日线附近承接，缩量回踩优于放量下杀。\n"
            f"- 放弃条件：跌破关键均线、板块龙头转弱、消息面出现明显利空。\n"
            f"- 持仓管理：计划持有 5 到 10 天，若放量跌破 5 日线先减仓，跌破止损位执行离场。\n"
            f"- 止损位：{candidate.get('stop_loss')}；止盈位：{candidate.get('take_profit')}\n"
            f"- 消息面备注：{review_text or '暂无额外负面消息，保持中性。'}\n"
        )

    @staticmethod
    def _build_run_summary_markdown(
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
            f"- 范围：沪深主板（非 ST）",
            f"- A股情绪：{market_snapshot.get('summary')}",
            f"- 美股情绪：{us_snapshot.get('summary')}",
            "",
            "## 候选股",
        ]
        if not selected:
            lines.append("- 无符合条件的标的")
            return "\n".join(lines)
        for item in selected:
            lines.append(
                f"- {item['rank']}. {item['name']}({item['code']}) | 分数 {item['score']} | {item['setup_type']} | {item['analysis_summary']}"
            )
        return "\n".join(lines)

    def _build_notification_markdown(self, payload: Dict[str, Any]) -> str:
        scan_date = payload.get("scan_date") or date.today().isoformat()
        strategy_name = payload.get("strategy_name") or payload.get("strategy_id") or "stock picker"
        market_snapshot = payload.get("market_snapshot") or {}
        us_snapshot = payload.get("us_market_snapshot") or {}
        candidates = payload.get("candidates") or []

        lines = [
            f"# {scan_date} 收盘后选股",
            "",
            f"- 策略：{strategy_name}",
            "- 范围：沪深主板（非 ST）",
            f"- A股情绪：{market_snapshot.get('summary') or market_snapshot.get('regime') or '中性'}",
            f"- 美股情绪：{us_snapshot.get('summary') or us_snapshot.get('mood') or '中性'}",
            f"- 扫描结果：入选 {payload.get('selected_count', 0)} / 命中 {payload.get('matched_count', 0)} / 扫描 {payload.get('total_scanned', 0)}",
            "",
        ]

        summary_markdown = payload.get("summary_markdown")
        if summary_markdown:
            lines.extend(["## 摘要", "", summary_markdown.strip(), ""])

        if not candidates:
            lines.extend(["## 候选股", "", "- 今日无符合条件的标的。"])
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
                lines.append(f"- 触发原因：{'，'.join(str(item) for item in reasons[:6])}")
            if candidate.get("stop_loss") is not None or candidate.get("take_profit") is not None:
                lines.append(
                    f"- 风控：止损 {candidate.get('stop_loss') or '-'}，止盈 {candidate.get('take_profit') or '-'}"
                )
            lines.extend(["", candidate.get("action_plan_markdown") or "暂无次日操作手册。", "", "---", ""])

        return "\n".join(lines).strip()
