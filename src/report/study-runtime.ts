export type StudyRuntimeMode = 'campaign' | 'arm' | 'observe' | 'continuous-pilot' | 'smoke' | 'preflight';
export type StudyRuntimeBlock = 'early' | 'late' | null;

export interface StudyRuntimeSnapshot {
  checkedAt: string;
  available: boolean;
  reason: string | null;
  runs: Array<{
    runId: string;
    mode: StudyRuntimeMode;
    block: StudyRuntimeBlock;
    status: string;
    captureJobRunning: boolean;
    captureCommandRunning: boolean;
    preparing: boolean;
    queued: boolean;
    startedAt: string | null;
  }>;
}

type WorkflowRun = { id: number; display_title: string; head_branch: string; status: string; run_started_at?: unknown };
type Job = { name: string; status: string; steps?: Array<{ name: string; status: string }> };
type Fetch = typeof fetch;

const API = 'https://api.github.com';
const WORKFLOW = 'market-study.yml';
const USER_AGENT = 'tradingbot-study-runtime/1.0';
const MAX_ACTIVE_RUNS = 10;
const waitingStatuses = new Set(['queued', 'waiting', 'pending', 'requested']);
const repositoryPattern = /^[A-Za-z0-9][A-Za-z0-9_.-]*\/[A-Za-z0-9][A-Za-z0-9_.-]*$/;
const branchPattern = /^[A-Za-z0-9][A-Za-z0-9._/-]*$/;
const titlePattern = /^Market study \/ (arm|campaign|observe|continuous-pilot|smoke|preflight) \/ (early|late)$/;
const captureStepNames = new Set([
  'Verify private destination and capture the owned block',
  'Verify sandbox access or run market smoke with private archive',
]);

function snapshot(now: number, available: boolean, reason: string | null,
  runs: StudyRuntimeSnapshot['runs'] = []): StudyRuntimeSnapshot {
  return { checkedAt: new Date(now).toISOString(), available, reason, runs };
}

function validBranch(value: string): boolean {
  return branchPattern.test(value) && !value.includes('..') && !value.startsWith('/') && !value.endsWith('/');
}

function safeStartedAt(value: unknown): string | null {
  if (typeof value !== 'string' || !Number.isFinite(Date.parse(value))) return null;
  return value;
}

function parseRuns(payload: unknown): { total: number; runs: WorkflowRun[] } | null {
  if (!payload || typeof payload !== 'object') return null;
  const value = payload as { total_count?: unknown; workflow_runs?: unknown };
  if (!Number.isSafeInteger(value.total_count) || (value.total_count as number) < 0 || !Array.isArray(value.workflow_runs)) return null;
  const runs: WorkflowRun[] = [];
  for (const raw of value.workflow_runs) {
    if (!raw || typeof raw !== 'object') return null;
    const run = raw as Partial<WorkflowRun>;
    if (!Number.isSafeInteger(run.id) || (run.id as number) <= 0 || typeof run.display_title !== 'string'
      || typeof run.head_branch !== 'string' || typeof run.status !== 'string') return null;
    runs.push(run as WorkflowRun);
  }
  return { total: value.total_count as number, runs };
}

function parseJobs(payload: unknown): { total: number; jobs: Job[] } | null {
  if (!payload || typeof payload !== 'object') return null;
  const value = payload as { total_count?: unknown; jobs?: unknown };
  if (!Number.isSafeInteger(value.total_count) || (value.total_count as number) < 0 || !Array.isArray(value.jobs)) return null;
  const jobs: Job[] = [];
  for (const raw of value.jobs) {
    if (!raw || typeof raw !== 'object') return null;
    const job = raw as { name?: unknown; status?: unknown; steps?: unknown };
    if (typeof job.name !== 'string' || typeof job.status !== 'string'
      || (job.steps !== undefined && !Array.isArray(job.steps))) return null;
    const steps: Array<{ name: string; status: string }> = [];
    for (const step of job.steps ?? []) {
      if (!step || typeof step !== 'object' || typeof (step as { name?: unknown }).name !== 'string'
        || typeof (step as { status?: unknown }).status !== 'string') return null;
      steps.push(step as { name: string; status: string });
    }
    jobs.push({ name: job.name, status: job.status, steps });
  }
  return { total: value.total_count as number, jobs };
}

async function getJson(fetcher: Fetch, path: string, token: string): Promise<{ ok: true; body: unknown } | { ok: false; reason: string }> {
  const abort = new AbortController();
  const timer = setTimeout(() => abort.abort(), 8_000);
  try {
    const response = await fetcher(`${API}${path}`, { signal: abort.signal, headers: {
      Accept: 'application/vnd.github+json', Authorization: `Bearer ${token}`, 'User-Agent': USER_AGENT,
      'X-GitHub-Api-Version': '2022-11-28',
    } });
    if (!response.ok) return { ok: false, reason: response.status === 401 || response.status === 403 ? 'AUTHENTICATION_FAILED' : 'API_UNAVAILABLE' };
    try { return { ok: true, body: await response.json() }; } catch { return { ok: false, reason: 'MALFORMED_RESPONSE' }; }
  } catch { return { ok: false, reason: 'NETWORK_UNAVAILABLE' }; }
  finally { clearTimeout(timer); }
}

function classify(run: WorkflowRun, jobs: Job[] | null): StudyRuntimeSnapshot['runs'][number] {
  const match = titlePattern.exec(run.display_title)!;
  const mode = match[1] as StudyRuntimeMode;
  const queued = waitingStatuses.has(run.status) || Boolean(jobs?.some(job => waitingStatuses.has(job.status)));
  const preparing = jobs?.some(job => job.name === 'prepare' && job.status === 'in_progress') ?? false;
  const captureName = mode === 'campaign' || mode === 'arm' ? 'campaign' : 'utility';
  const capture = jobs?.find(job => job.name === captureName) ?? null;
  const captureJobRunning = capture?.status === 'in_progress';
  const captureCommandRunning = captureJobRunning && Boolean(capture?.steps?.some(step => captureStepNames.has(step.name) && step.status === 'in_progress'));
  return { runId: String(run.id), mode, block: (mode === 'campaign' || mode === 'arm') ? match[2] as StudyRuntimeBlock : null,
    status: run.status, captureJobRunning, captureCommandRunning, preparing, queued, startedAt: safeStartedAt(run.run_started_at) };
}

/** Reads only public GitHub Actions metadata. Missing or incomplete evidence is unavailable, never idle. */
export async function readStudyRuntime(environment: NodeJS.ProcessEnv, fetcher: Fetch = fetch, now = Date.now()): Promise<StudyRuntimeSnapshot> {
  const repository = environment.GITHUB_REPOSITORY?.trim() ?? '';
  const branch = environment.STUDY_DEFAULT_BRANCH?.trim() || 'main';
  const token = environment.STUDY_ACTIONS_TOKEN?.trim() ?? '';
  if (!repositoryPattern.test(repository) || !validBranch(branch) || !token) return snapshot(now, false, 'CONFIGURATION_UNAVAILABLE');

  const collected = new Map<number, WorkflowRun>();
  for (const status of ['in_progress', ...waitingStatuses]) {
    const response = await getJson(fetcher, `/repos/${repository}/actions/workflows/${WORKFLOW}/runs?status=${status}&branch=${encodeURIComponent(branch)}&per_page=100`, token);
    if (!response.ok) return snapshot(now, false, response.reason);
    const parsed = parseRuns(response.body);
    if (!parsed) return snapshot(now, false, 'MALFORMED_RESPONSE');
    if (parsed.total > parsed.runs.length || parsed.runs.length > 100) return snapshot(now, false, 'TRUNCATED_RESPONSE');
    for (const run of parsed.runs) collected.set(run.id, run);
  }
  const trusted = [...collected.values()].filter(run => run.head_branch === branch && titlePattern.test(run.display_title));
  if (trusted.length > MAX_ACTIVE_RUNS) return snapshot(now, false, 'ACTIVE_RUN_LIMIT');

  const result: StudyRuntimeSnapshot['runs'] = [];
  for (const run of trusted.sort((left, right) => left.id - right.id)) {
    let jobs: Job[] | null = null;
    if (run.status === 'in_progress') {
      const response = await getJson(fetcher, `/repos/${repository}/actions/runs/${run.id}/jobs?per_page=100`, token);
      if (!response.ok) return snapshot(now, false, response.reason);
      const parsed = parseJobs(response.body);
      if (!parsed) return snapshot(now, false, 'MALFORMED_RESPONSE');
      if (parsed.total > parsed.jobs.length || parsed.jobs.length > 100) return snapshot(now, false, 'TRUNCATED_RESPONSE');
      jobs = parsed.jobs;
    }
    result.push(classify(run, jobs));
  }
  return snapshot(now, true, null, result);
}
