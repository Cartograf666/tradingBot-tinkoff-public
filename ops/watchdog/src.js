const API = "https://api.github.com";
const OWNER = "Cartograf666";
const REPOSITORY = "tradingBot-tinkoff-public";
const WORKFLOW = "market-study.yml";
const REF = "main";
const TIME_ZONE = "Europe/Moscow";
const MAX_PAGES = 3;
const PER_PAGE = 100;
const ACTIVE_STATUSES = new Set(["queued", "in_progress", "waiting", "pending", "requested", "action_required"]);

function moscow(now) {
  const values = Object.fromEntries(new Intl.DateTimeFormat("en-CA", {
    timeZone: TIME_ZONE,
    year: "numeric", month: "2-digit", day: "2-digit", weekday: "short",
    hour: "2-digit", minute: "2-digit", hourCycle: "h23"
  }).formatToParts(now).filter(({ type }) => type !== "literal").map(({ type, value }) => [type, value]));
  return { date: `${values.year}-${values.month}-${values.day}`, weekday: values.weekday, minute: Number(values.hour) * 60 + Number(values.minute) };
}

function isBusinessDay(local) {
  return local.weekday !== "Sat" && local.weekday !== "Sun";
}

function blockForRun(run) {
  const title = String(run.display_title || run.name || "").toLowerCase();
  if (/\/(?:\s*)?(?:arm|campaign)(?:\s*)\/\s*early\b/.test(title)) return "early";
  if (/\/(?:\s*)?(?:arm|campaign)(?:\s*)\/\s*late\b/.test(title)) return "late";
  return null;
}

function modeForRun(run) {
  const title = String(run.display_title || run.name || "").toLowerCase();
  if (/\/\s*report(?:\s|\/|$)/.test(title)) return "report";
  if (/\/\s*arm(?:\s|\/|$)/.test(title)) return "arm";
  if (/\/\s*campaign(?:\s|\/|$)/.test(title)) return "campaign";
  return null;
}

function createdOnDate(run, date) {
  return typeof run.created_at === "string" && moscow(new Date(run.created_at)).date === date;
}

function reportSessionForRun(run) {
  if (typeof run.created_at !== "string") return null;
  // Dispatch metadata is created at request time. Noon separates this Worker's
  // pre-market and post-market report windows without new workflow inputs.
  return moscow(new Date(run.created_at)).minute < 12 * 60 ? "morning" : "evening";
}

export function selectAction(now) {
  const local = moscow(now);
  if (!isBusinessDay(local)) return null;
  const windows = [
    { block: "early", arm: 8 * 60 + 20, campaign: 8 * 60 + 50, close: 9 * 60 + 15 },
    { block: "late", arm: 13 * 60 + 20, campaign: 13 * 60 + 50, close: 14 * 60 + 15 }
  ];
  for (const window of windows) {
    if (local.minute >= window.arm && local.minute < window.campaign) return { mode: "arm", block: window.block, date: local.date };
    if (local.minute >= window.campaign && local.minute <= window.close) return { mode: "campaign", block: window.block, date: local.date };
  }
  // Morning recovery must finish before the early arm window. The report workflow
  // itself owns recovery of old incomplete private dates.
  if (local.minute >= 8 * 60 + 10 && local.minute <= 8 * 60 + 15) return { mode: "report", reportSession: "morning", date: local.date };
  if (local.minute >= 19 * 60 + 20 && local.minute <= 19 * 60 + 35) return { mode: "report", reportSession: "evening", date: local.date };
  return null;
}

function apiHeaders(token) {
  return { Accept: "application/vnd.github+json", Authorization: `Bearer ${token}`, "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "market-study-watchdog" };
}

async function github(fetcher, url, options, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetcher(url, { ...options, signal: controller.signal });
    if (!response.ok) throw new Error(`github_${response.status}`);
    return response;
  } finally {
    clearTimeout(timer);
  }
}

function previousMoscowDate(date) {
  const noon = new Date(`${date}T12:00:00Z`);
  noon.setUTCDate(noon.getUTCDate() - 1);
  return noon.toISOString().slice(0, 10);
}

async function listRunQuery(fetcher, token, parameters, timeoutMs) {
  const runs = [];
  let total = null;
  for (let page = 1; page <= MAX_PAGES; page += 1) {
    const query = new URLSearchParams({ branch: REF, per_page: String(PER_PAGE), page: String(page), ...parameters });
    const url = `${API}/repos/${OWNER}/${REPOSITORY}/actions/workflows/${WORKFLOW}/runs?${query}`;
    const response = await github(fetcher, url, { headers: apiHeaders(token) }, timeoutMs);
    const body = await response.json();
    if (!Array.isArray(body.workflow_runs) || !Number.isInteger(body.total_count)) throw new Error("github_invalid_runs_response");
    total = body.total_count;
    runs.push(...body.workflow_runs);
    if (body.workflow_runs.length < PER_PAGE) break;
  }
  if (total > runs.length) throw new Error("github_runs_truncated");
  return runs;
}

export async function listRuns(fetcher, token, action, timeoutMs = 8_000) {
  // A date-scoped history avoids an ever-growing workflow history. Active jobs get
  // a separate unscoped read, so an old queued job can never be silently ignored.
  const since = `${previousMoscowDate(action.date)}T00:00:00+03:00`;
  const recent = await listRunQuery(fetcher, token, { created: `>=${since}` }, timeoutMs);
  const active = [];
  for (const status of ACTIVE_STATUSES) {
    active.push(...await listRunQuery(fetcher, token, { status }, timeoutMs));
  }
  const seenIds = new Set();
  return [...recent, ...active].filter((run) => {
    if (run.id == null) return true;
    if (seenIds.has(run.id)) return false;
    seenIds.add(run.id);
    return true;
  });
}

export function decisionFor(action, runs) {
  const matching = action.mode === "report"
    ? runs.filter((run) => modeForRun(run) === "report")
    : runs.filter((run) => blockForRun(run) === action.block && ["arm", "campaign"].includes(modeForRun(run)));
  if (matching.some((run) => ACTIVE_STATUSES.has(run.status))) return { dispatch: false, reason: "active" };
  const today = matching.filter((run) => createdOnDate(run, action.date) &&
    (action.mode !== "report" || reportSessionForRun(run) === action.reportSession));
  if (today.some((run) => run.status === "completed" && run.conclusion === "success")) return { dispatch: false, reason: "success_observed" };
  const limit = action.mode === "report" ? 2 : 3;
  if (today.length >= limit) return { dispatch: false, reason: "attempt_limit" };
  return { dispatch: true, reason: "dispatch" };
}

export async function dispatch(fetcher, token, action, timeoutMs = 8_000) {
  const inputs = action.mode === "report" ? { mode: "report", block: "early" } : { mode: action.mode, block: action.block };
  const url = `${API}/repos/${OWNER}/${REPOSITORY}/actions/workflows/${WORKFLOW}/dispatches`;
  await github(fetcher, url, {
    method: "POST", headers: { ...apiHeaders(token), "Content-Type": "application/json" },
    body: JSON.stringify({ ref: REF, inputs })
  }, timeoutMs);
}

export async function runWatchdog(env, now = new Date(), fetcher = fetch) {
  const action = selectAction(now);
  if (!action) return { action: "none" };
  if (env.WATCHDOG_ENABLED !== "true") return { action: "skipped", reason: "disabled" };
  if (!env.GITHUB_DISPATCH_TOKEN) return { action: "skipped", reason: "not_configured" };
  let runs;
  try {
    runs = await listRuns(fetcher, env.GITHUB_DISPATCH_TOKEN, action);
  } catch {
    return { action: "skipped", reason: "github_unavailable" };
  }
  const decision = decisionFor(action, runs);
  if (!decision.dispatch) return { action: "skipped", reason: decision.reason };
  try {
    await dispatch(fetcher, env.GITHUB_DISPATCH_TOKEN, action);
    return { action: "dispatched", mode: action.mode, block: action.block || null };
  } catch {
    return { action: "skipped", reason: "dispatch_failed" };
  }
}

export default {
  async scheduled(_event, env, ctx) {
    ctx.waitUntil((async () => {
      const outcome = await runWatchdog(env);
      console.log(JSON.stringify({ watchdog: outcome }));
      if (outcome.reason === "github_unavailable" || outcome.reason === "dispatch_failed") throw new Error(`watchdog_${outcome.reason}`);
    })());
  },
  fetch(request, env) {
    // Deliberately exposes only non-secret readiness/configuration state.
    if (request.method !== "GET" || new URL(request.url).pathname !== "/health") return new Response("Not found", { status: 404 });
    return Response.json({ ok: true, enabled: env.WATCHDOG_ENABLED === "true", configured: Boolean(env.GITHUB_DISPATCH_TOKEN) });
  }
};
