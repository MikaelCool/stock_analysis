# -*- coding: utf-8 -*-
"""Stock picker API endpoints."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.services.stock_picker_service import StockPickerService

router = APIRouter()


class StrategyOption(BaseModel):
    strategy_id: str
    name: str
    description: str
    skill_id: str
    category: str
    params: Dict[str, Any]


class StrategyListResponse(BaseModel):
    strategies: List[StrategyOption]


class PickerScanRequest(BaseModel):
    strategy_id: Optional[str] = None
    scan_date: Optional[date] = None
    max_candidates: Optional[int] = Field(default=None, ge=1, le=10)
    strategy_params: Dict[str, Any] = Field(default_factory=dict)
    send_notification: bool = True
    force_refresh: bool = False


class PickerRunSummary(BaseModel):
    id: int
    query_id: str
    strategy_id: str
    strategy_name: str
    scan_date: Optional[str] = None
    status: str
    total_scanned: int
    matched_count: int
    selected_count: int
    market_snapshot: Optional[Dict[str, Any]] = None
    us_market_snapshot: Optional[Dict[str, Any]] = None
    optimization: Optional[Dict[str, Any]] = None
    summary_markdown: Optional[str] = None
    created_at: Optional[str] = None
    completed_at: Optional[str] = None


class PickerCandidate(BaseModel):
    id: int
    run_id: int
    code: str
    name: Optional[str] = None
    strategy_id: str
    scan_date: Optional[str] = None
    setup_type: str
    score: float
    rank: int
    selected: bool
    operation_advice: Optional[str] = None
    analysis_summary: Optional[str] = None
    action_plan_markdown: Optional[str] = None
    reasons: List[str] = Field(default_factory=list)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    market_context: Dict[str, Any] = Field(default_factory=dict)
    news_context: Optional[Any] = None
    llm_model: Optional[str] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    backtests: List[Dict[str, Any]] = Field(default_factory=list)
    created_at: Optional[str] = None


class PickerRunDetail(PickerRunSummary):
    candidates: List[PickerCandidate] = Field(default_factory=list)
    backtest_stats: Optional[Dict[str, Any]] = None


class MarketSentimentResponse(BaseModel):
    market: str
    score: float
    regime: str
    summary: str
    breadth: Optional[float] = None
    limit_up_count: Optional[float] = None
    limit_down_count: Optional[float] = None
    updated_at: Optional[str] = None


class PickerStrategyDistributionItem(BaseModel):
    strategy_id: str
    strategy_name: str
    total_evaluations: int
    pending_count: int
    completed_count: int
    win_count: int
    loss_count: int
    neutral_count: int
    win_rate_pct: Optional[float] = None
    avg_return_pct: Optional[float] = None
    avg_max_drawdown_pct: Optional[float] = None
    worst_drawdown_pct: Optional[float] = None


class PickerSetupTypeDistributionItem(BaseModel):
    setup_type: str
    total_evaluations: int
    pending_count: int
    completed_count: int
    win_count: int
    loss_count: int
    neutral_count: int
    win_rate_pct: Optional[float] = None
    avg_return_pct: Optional[float] = None
    avg_max_drawdown_pct: Optional[float] = None
    worst_drawdown_pct: Optional[float] = None


class PickerBacktestSummaryResponse(BaseModel):
    strategy_id: Optional[str] = None
    strategy_name: Optional[str] = None
    horizon_days: int
    top_n: int
    total_evaluations: int
    pending_count: int
    completed_count: int
    win_count: int
    loss_count: int
    neutral_count: int
    win_rate_pct: Optional[float] = None
    avg_return_pct: Optional[float] = None
    avg_max_drawdown_pct: Optional[float] = None
    worst_drawdown_pct: Optional[float] = None
    scan_count: int
    stock_count: int
    latest_scan_date: Optional[str] = None
    strategy_distribution: List[PickerStrategyDistributionItem] = Field(default_factory=list)
    setup_type_distribution: List[PickerSetupTypeDistributionItem] = Field(default_factory=list)


class PickerBacktestResultItem(BaseModel):
    id: int
    candidate_id: int
    run_id: int
    strategy_id: str
    strategy_name: str
    code: str
    name: Optional[str] = None
    scan_date: Optional[str] = None
    rank: int
    score: float
    setup_type: str
    horizon_days: int
    status: str
    entry_date: Optional[str] = None
    exit_date: Optional[str] = None
    entry_price: Optional[float] = None
    exit_price: Optional[float] = None
    return_pct: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    outcome: Optional[str] = None


class PickerBacktestResultsResponse(BaseModel):
    total: int
    page: int
    limit: int
    items: List[PickerBacktestResultItem] = Field(default_factory=list)


class PickerBacktestRunRequest(BaseModel):
    strategy_id: Optional[str] = None
    horizon_days: int = Field(default=5, ge=1, le=10)
    top_n: int = Field(default=5, ge=1, le=10)
    max_candidates: int = Field(default=2000, ge=50, le=10000)


class PickerBacktestRunResponse(BaseModel):
    processed: int
    completed: int
    pending: int
    summary: PickerBacktestSummaryResponse


@router.get("/strategies", response_model=StrategyListResponse)
async def get_picker_strategies():
    return StrategyListResponse(strategies=[StrategyOption(**item) for item in StockPickerService.list_strategies()])


@router.get("/runs", response_model=List[PickerRunSummary])
async def list_picker_runs(limit: int = 20):
    service = StockPickerService()
    return [PickerRunSummary(**item) for item in service.list_runs(limit=limit)]


@router.get("/runs/{run_id}", response_model=PickerRunDetail)
async def get_picker_run(run_id: int):
    service = StockPickerService()
    payload = service.get_run_detail(run_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="picker run not found")
    return PickerRunDetail(**payload)


@router.get("/market-sentiment", response_model=MarketSentimentResponse)
async def get_picker_market_sentiment():
    service = StockPickerService()
    return MarketSentimentResponse(**service.get_market_sentiment())


@router.post("/scan", response_model=PickerRunDetail)
async def run_picker_scan(request: PickerScanRequest):
    service = StockPickerService()
    try:
        payload = service.run_scan(
            strategy_id=request.strategy_id,
            scan_date=request.scan_date,
            max_candidates=request.max_candidates,
            strategy_params=request.strategy_params,
            send_notification=request.send_notification,
            force_refresh=request.force_refresh,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return PickerRunDetail(**payload)


@router.post("/backtest")
async def backfill_picker_backtests(strategy_id: Optional[str] = None):
    service = StockPickerService()
    return service.backfill_backtests(strategy_id=strategy_id)


@router.get("/backtest/summary", response_model=PickerBacktestSummaryResponse)
async def get_picker_backtest_summary(
    strategy_id: Optional[str] = None,
    horizon_days: int = 5,
    scan_date_from: Optional[date] = None,
    scan_date_to: Optional[date] = None,
    top_n: int = 5,
):
    service = StockPickerService()
    return PickerBacktestSummaryResponse(
        **service.get_strategy_backtest_summary(
            strategy_id=strategy_id,
            horizon_days=horizon_days,
            scan_date_from=scan_date_from,
            scan_date_to=scan_date_to,
            top_n=top_n,
        )
    )


@router.get("/backtest/results", response_model=PickerBacktestResultsResponse)
async def get_picker_backtest_results(
    strategy_id: Optional[str] = None,
    horizon_days: int = 5,
    scan_date_from: Optional[date] = None,
    scan_date_to: Optional[date] = None,
    top_n: int = 5,
    page: int = 1,
    limit: int = 30,
):
    service = StockPickerService()
    return PickerBacktestResultsResponse(
        **service.get_strategy_backtest_results(
            strategy_id=strategy_id,
            horizon_days=horizon_days,
            scan_date_from=scan_date_from,
            scan_date_to=scan_date_to,
            top_n=top_n,
            page=page,
            limit=limit,
        )
    )


@router.post("/backtest/run", response_model=PickerBacktestRunResponse)
async def run_picker_backtest_refresh(request: PickerBacktestRunRequest):
    service = StockPickerService()
    payload = service.run_strategy_backtest_refresh(
        strategy_id=request.strategy_id,
        horizon_days=request.horizon_days,
        top_n=request.top_n,
        max_candidates=request.max_candidates,
    )
    return PickerBacktestRunResponse(**payload)
