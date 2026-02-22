export interface RunFilters {
  icp?: string;            // ICP slug, e.g. 'source-angel'
  priority?: 'HIGH' | 'MEDIUM' | 'LOW' | 'ALL';
  minScore?: number;
  source?: string;
  eeRisk?: string;
  temperature?: string;
  limit?: number;
  forceRefresh?: boolean;
  forceRegen?: boolean;
  minEmployees?: number | null;
  maxEmployees?: number | null;
}

export type LogFn = (msg: string) => void;
