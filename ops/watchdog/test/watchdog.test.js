import test from "node:test";
import assert from "node:assert/strict";
import worker, { decisionFor, listRuns, runWatchdog, selectAction } from "../src.js";

const at = (iso) => new Date(iso);
const run = (overrides = {}) => ({ display_title: "Market study / arm / early", created_at: "2026-09-16T05:20:00Z", status: "completed", conclusion: "failure", ...overrides });
const response = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const fetchWithRuns = (runs, status = 200) => {
  const calls = [];
  const fetcher = async (url, options = {}) => {
    calls.push({ url, options });
    if (url.includes("/dispatches")) return response({}, status);
    const requestedStatus = new URL(url).searchParams.get("status");
    const selected = requestedStatus ? runs.filter((item) => item.status === requestedStatus) : runs;
    return response({ total_count: selected.length, workflow_runs: selected }, status);
  };
  return { fetcher, calls };
};

test("window selection handles business dates and arm/campaign boundaries", () => {
  assert.deepEqual(selectAction(at("2026-09-16T05:20:00Z")), { mode: "arm", block: "early", date: "2026-09-16" });
  assert.deepEqual(selectAction(at("2026-09-16T05:50:00Z")), { mode: "campaign", block: "early", date: "2026-09-16" });
  assert.equal(selectAction(at("2026-09-16T06:16:00Z")), null);
  assert.deepEqual(selectAction(at("2026-09-16T05:10:00Z")), { mode: "report", reportSession: "morning", date: "2026-09-16" });
  assert.deepEqual(selectAction(at("2026-09-16T05:15:00Z")), { mode: "report", reportSession: "morning", date: "2026-09-16" });
  assert.deepEqual(selectAction(at("2026-09-16T16:20:00Z")), { mode: "report", reportSession: "evening", date: "2026-09-16" });
  assert.deepEqual(selectAction(at("2026-09-16T16:35:00Z")), { mode: "report", reportSession: "evening", date: "2026-09-16" });
  assert.equal(selectAction(at("2026-09-19T05:20:00Z")), null);
});

test("missing credentials and disabled configuration cannot dispatch", async () => {
  let outcome = await runWatchdog({ WATCHDOG_ENABLED: "false", GITHUB_DISPATCH_TOKEN: "token" }, at("2026-09-16T05:20:00Z"));
  assert.equal(outcome.reason, "disabled");
  outcome = await runWatchdog({ WATCHDOG_ENABLED: "true" }, at("2026-09-16T05:20:00Z"));
  assert.equal(outcome.reason, "not_configured");
});

test("GitHub auth/read failures fail closed before dispatch", async () => {
  const { fetcher, calls } = fetchWithRuns([], 401);
  const outcome = await runWatchdog({ WATCHDOG_ENABLED: "true", GITHUB_DISPATCH_TOKEN: "token" }, at("2026-09-16T05:20:00Z"), fetcher);
  assert.equal(outcome.reason, "github_unavailable");
  assert.equal(calls.filter(({ url }) => url.includes("dispatches")).length, 0);
  assert.equal(calls[0].options.headers["User-Agent"], "market-study-watchdog");
});

test("active duplicate and completed success suppress market launches", () => {
  const action = { mode: "arm", block: "early", date: "2026-09-16" };
  assert.equal(decisionFor(action, [run({ status: "in_progress", conclusion: null })]).reason, "active");
  assert.equal(decisionFor(action, [run({ conclusion: "success" })]).reason, "success_observed");
});

test("failed market retries are bounded and late window dispatches campaign", async () => {
  const action = { mode: "campaign", block: "early", date: "2026-09-16" };
  assert.equal(decisionFor(action, [run(), run(), run()]).reason, "attempt_limit");
  const { fetcher, calls } = fetchWithRuns([run({ display_title: "Market study / arm / early" })]);
  const outcome = await runWatchdog({ WATCHDOG_ENABLED: "true", GITHUB_DISPATCH_TOKEN: "token" }, at("2026-09-16T05:50:00Z"), fetcher);
  assert.equal(outcome.action, "dispatched");
  const body = JSON.parse(calls.at(-1).options.body);
  assert.deepEqual(body.inputs, { mode: "campaign", block: "early" });
});

test("failed report retries are bounded and report success is metadata-only", () => {
  const action = { mode: "report", reportSession: "morning", date: "2026-09-16" };
  const failed = run({ display_title: "Market study / report / early" });
  assert.equal(decisionFor(action, [failed]).dispatch, true);
  assert.equal(decisionFor(action, [failed, failed]).reason, "attempt_limit");
  assert.equal(decisionFor(action, [run({ display_title: "Market study / report / early", conclusion: "success" })]).reason, "success_observed");
});

test("morning report success does not suppress the evening report session", () => {
  const morningSuccess = run({ display_title: "Market study / report / early", created_at: "2026-09-16T05:10:00Z", conclusion: "success" });
  assert.equal(decisionFor({ mode: "report", reportSession: "morning", date: "2026-09-16" }, [morningSuccess]).reason, "success_observed");
  assert.equal(decisionFor({ mode: "report", reportSession: "evening", date: "2026-09-16" }, [morningSuccess]).dispatch, true);
});

test("history is date-scoped while old active runs remain visible", async () => {
  const calls = [];
  const active = run({ id: 7, status: "in_progress", conclusion: null, created_at: "2026-08-01T05:20:00Z" });
  const fetcher = async (url) => {
    calls.push(url);
    const params = new URL(url).searchParams;
    if (params.get("created")) return response({ total_count: 0, workflow_runs: [] });
    return response({ total_count: params.get("status") === "in_progress" ? 1 : 0, workflow_runs: params.get("status") === "in_progress" ? [active] : [] });
  };
  const runs = await listRuns(fetcher, "token", { mode: "arm", block: "early", date: "2026-09-16" });
  assert.deepEqual(runs, [active]);
  assert.match(calls[0], /created=%3E%3D2026-09-15T00%3A00%3A00%2B03%3A00/);
  assert.equal(decisionFor({ mode: "arm", block: "early", date: "2026-09-16" }, runs).reason, "active");
});

test("truncated current history fails closed", async () => {
  const hundred = Array.from({ length: 100 }, (_, id) => ({ id, status: "completed", conclusion: "failure" }));
  const fetcher = async (url) => {
    const params = new URL(url).searchParams;
    if (params.get("created")) return response({ total_count: 301, workflow_runs: hundred });
    throw new Error("active query must not run after a truncated history");
  };
  await assert.rejects(() => listRuns(fetcher, "token", { mode: "arm", block: "early", date: "2026-09-16" }), /github_runs_truncated/);
});

test("the public surface is health-only and never returns the token", async () => {
  const env = { WATCHDOG_ENABLED: "true", GITHUB_DISPATCH_TOKEN: "private-token" };
  const health = await worker.fetch(new Request("https://watchdog.example/health"), env);
  assert.deepEqual(await health.json(), { ok: true, enabled: true, configured: true });
  const trigger = await worker.fetch(new Request("https://watchdog.example/dispatch", { method: "POST" }), env);
  assert.equal(trigger.status, 404);
});

test("downstream failures reject the scheduled invocation after a safe diagnostic log", async () => {
  const originalFetch = globalThis.fetch;
  const originalLog = console.log;
  const OriginalDate = globalThis.Date;
  const logs = [];
  let pending;
  globalThis.fetch = async () => response({}, 503);
  globalThis.Date = class extends OriginalDate {
    constructor(...args) { super(...(args.length ? args : ["2026-09-16T05:20:00Z"])); }
  };
  console.log = (line) => logs.push(line);
  try {
    worker.scheduled({}, { WATCHDOG_ENABLED: "true", GITHUB_DISPATCH_TOKEN: "token" }, { waitUntil: (promise) => { pending = promise; } });
    await assert.rejects(pending, /watchdog_github_unavailable/);
    assert.deepEqual(JSON.parse(logs[0]), { watchdog: { action: "skipped", reason: "github_unavailable" } });
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.Date = OriginalDate;
    console.log = originalLog;
  }
});
