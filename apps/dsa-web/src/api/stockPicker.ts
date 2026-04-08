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
    const response = await apiClient.get('/api/v1/picker/market-sentiment');
    return toCamel<PickerMarketSentiment>(response.data);
  },
  async scan(payload: PickerScanRequest): Promise<PickerRun> {
    const response = await apiClient.post('/api/v1/picker/scan', {
      strategy_id: payload.strategyId,
      max_candidates: payload.maxCandidates,
      strategy_params: payload.strategyParams ?? {},
      send_notification: payload.sendNotification ?? false,
      force_refresh: payload.forceRefresh ?? false,
    });
    return toCamel<PickerRun>(response.data);
  },
};
