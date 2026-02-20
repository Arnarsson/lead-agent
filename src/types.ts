export interface RunFilters {
  priority?: 'HIGH' | 'MEDIUM' | 'LOW' | 'ALL';
  minScore?: number;           // default 70
  source?: string;             // 'ALL' or 'TheHub' | 'LinkedIn' | 'IT-Jobbank' | 'Jobindex'
  eeRisk?: string;             // 'LOW' | 'LOW,MEDIUM' | 'ALL'  (comma-separated)
  temperature?: string;        // 'HOT' | 'HOT,WARM' | 'ALL'   (comma-separated)
  limit?: number;              // max companies per run (cost guard)
  forceRefresh?: boolean;      // bypass 7-day cache
}

export type LogFn = (msg: string) => void;
