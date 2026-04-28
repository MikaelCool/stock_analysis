import apiClient from './index';

export interface PickerStrategyOption {
  strategyId: string;
  name: string;
  description: string;
  skillId: string;
  category: string;
  params: Record<string, unknown>;
}

export interface PickerBacktest {
  id: number;
  horizonDays: number;
  status: string;
  entryDate?: string | null;
  exitDate?: string | null;
  returnPct?: number | null;
  maxDrawdownPct?: number | null;
  outcome?: string | null;
}

export interface PickerCandidate {
  id: number;
  code: string;
  name?: string | null;
  strategyId: string;
  scanDate?: string | null;
  setupType: string;
  score: number;
  rank: number;
  selected: boolean;
  operationAdvice?: string | null;
  analysisSummary?: string | null;
  actionPlanMarkdown?: string | null;
  reasons: string[];
  metrics: Record<string, unknown>;
  marketContext: Record<string, unknown>;
  newsContext?: unknown;
  llmModel?: string | null;
  stopLoss?: number | null;
  takeProfit?: number | null;
  backtests: PickerBacktest[];
}

export interface PickerRun {
  id: number;
  queryId: string;
  strategyId: string;
  strategyName: string;
  scanDate?: string | null;
  status: string;
  totalScanned: number;
  matchedCount: number;
  selectedCount: number;
  marketSnapshot?: Record<string, unknown> | null;
  usMarketSnapshot?: Record<string, unknown> | null;
  optimization?: Record<string, unknown> | null;
  summaryMarkdown?: string | null;
  createdAt?: string | null;
  completedAt?: string | null;
  candidates?: PickerCandidate[];
  backtestStats?: Record<string, unknown> | null;
}

export interface PickerMarketSentiment {
  market: string;
  score: number;
  regime: string;
  summary: string;
  breadth?: number | null;
  limitUpCount?: number | null;
  limitDownCount?: number | null;
  updatedAt?: string | null;
}

export interface PickerScanRequest {
  strategyId?: string;
  maxCandidates?: number;
  strategyParams?: Record<string, number | string>;
  sendNotification?: boolean;
  forceRefresh?: boolean;
}

export interface PickerStrategyDistributionItem {
  strategyId: string;
  strategyName: string;
  totalEvaluations: number;
  pendingCount: number;
  completedCount: number;
  winCount: number;
  lossCount: number;
  neutralCount: number;
  winRatePct?: number | null;
  avgReturnPct?: number | null;
  avgMaxDrawdownPct?: number | null;
  worstDrawdownPct?: number | null;
}

export interface PickerSetupTypeDistributionItem {
  setupType: string;
  totalEvaluations: number;
  pendingCount: number;
  completedCount: number;
  winCount: number;
  lossCount: number;
  neutralCount: number;
  winRatePct?: number | null;
  avgReturnPct?: number | null;
  avgMaxDrawdownPct?: number | null;
  worstDrawdownPct?: number | null;
}

export interface PickerBacktestSummary {
  strategyId?: string | null;
  strategyName?: string | null;
  horizonDays: number;
  topN: number;
  totalEvaluations: number;
  pendingCount: number;
  completedCount: number;
  winCount: number;
  lossCount: number;
  neutralCount: number;
  winRatePct?: number | null;
  avgReturnPct?: number | null;
  avgMaxDrawdownPct?: number | null;
  worstDrawdownPct?: number | null;
  scanCount: number;
  stockCount: number;
  latestScanDate?: string | null;
  strategyDistribution: PickerStrategyDistributionItem[];
  setupTypeDistribution: PickerSetupTypeDistributionItem[];
}

export interface PickerBacktestResultItem {
  id: number;
  candidateId: number;
  runId: number;
  strategyId: string;
  strategyName: string;
  code: string;
  name?: string | null;
  scanDate?: string | null;
  rank: number;
  score: number;
  setupType: string;
  horizonDays: number;
  status: string;
  entryDate?: string | null;
  exitDate?: string | null;
  entryPrice?: number | null;
  exitPrice?: number | null;
  returnPct?: number | null;
  maxDrawdownPct?: number | null;
  outcome?: string | null;
}

export interface PickerBacktestResultsResponse {
  total: number;
  page: number;
  limit: number;
  items: PickerBacktestResultItem[];
}

export interface PickerBacktestRunResponse {
  processed: number;
  completed: number;
  pending: number;
  summary: PickerBacktestSummary;
}

function toCamel<T>(value: unknown): T {
  if (Array.isArray(value)) {
    return value.map((item) => toCamel(item)) as T;
  }
  if (value && typeof value === 'object') {
    const next: Record<string, unknown> = {};
    Object.entries(value as Record<string, unknown>).forEach(([key, item]) => {
      const camelKey = key.replace(/_([a-z])/g, (_, char: string) => char.toUpperCase());
      next[camelKey] = toCamel(item);
    });
    return next as T;
  }
  return value as T;
}

export const stockPickerApi = {
  async getStrategies(): Promise<PickerStrategyOption[]> {
    const response = await apiClient.get('/api/v1/picker/strategies');
    return toCamel<{ strategies: PickerStrategyOption[] }>(response.data).strategies;
  },
  async listRuns(limit = 20): Promise<PickerRun[]> {
    const response = await apiClient.get('/api/v1/picker/runs', { params: { limit } });
    return toCamel<PickerRun[]>(response.data);
  },
  async getRun(runId: number): Promise<PickerRun> {
    const response = await apiClient.get(`/api/v1/picker/runs/${runId}`);
    return toCamel<PickerRun>(response.data);
  },
  async getMarketSentiment(): Promise<PickerMarketSentiment> {
    const response = await apiClient.get('/api/v1/picker/market-sentiment', {
      params: { _t: Date.now() },
    });
    return toCamel<PickerMarketSentiment>(response.data);
  },
  async scan(payload: PickerScanRequest): Promise<PickerRun> {
    const response = await apiClient.post('/api/v1/picker/scan', {
      strategy_id: payload.strategyId,
      max_candidates: payload.maxCandidates,
      strategy_params: payload.strategyParams ?? {},
      send_notification: payload.sendNotification ?? false,
      force_refresh: payload.forceRefresh ?? false,
    }, {
      // Full-market refresh on the first scan of the day can take minutes.
      // Keep the request open so the browser does not abort a healthy scan.
      timeout: 15 * 60 * 1000,
    });
    return toCamel<PickerRun>(response.data);
  },
  async getBacktestSummary(params: {
    strategyId?: string;
    horizonDays?: number;
    scanDateFrom?: string;
    scanDateTo?: string;
    topN?: number;
  } = {}): Promise<PickerBacktestSummary> {
    const response = await apiClient.get('/api/v1/picker/backtest/summary', {
      params: {
        strategy_id: params.strategyId,
        horizon_days: params.horizonDays,
        scan_date_from: params.scanDateFrom,
        scan_date_to: params.scanDateTo,
        top_n: params.topN,
      },
    });
    return toCamel<PickerBacktestSummary>(response.data);
  },
  async getBacktestResults(params: {
    strategyId?: string;
    horizonDays?: number;
    scanDateFrom?: string;
    scanDateTo?: string;
    topN?: number;
    page?: number;
    limit?: number;
  } = {}): Promise<PickerBacktestResultsResponse> {
    const response = await apiClient.get('/api/v1/picker/backtest/results', {
      params: {
        strategy_id: params.strategyId,
        horizon_days: params.horizonDays,
        scan_date_from: params.scanDateFrom,
        scan_date_to: params.scanDateTo,
        top_n: params.topN,
        page: params.page ?? 1,
        limit: params.limit ?? 30,
      },
    });
    return toCamel<PickerBacktestResultsResponse>(response.data);
  },
  async runBacktestRefresh(payload: {
    strategyId?: string;
    horizonDays?: number;
    topN?: number;
    maxCandidates?: number;
  } = {}): Promise<PickerBacktestRunResponse> {
    const response = await apiClient.post('/api/v1/picker/backtest/run', {
      strategy_id: payload.strategyId,
      horizon_days: payload.horizonDays ?? 5,
      top_n: payload.topN ?? 5,
      max_candidates: payload.maxCandidates ?? 2000,
    });
    return toCamel<PickerBacktestRunResponse>(response.data);
  },
};
