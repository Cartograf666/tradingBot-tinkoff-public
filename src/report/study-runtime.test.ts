import assert from 'node:assert/strict';
import test from 'node:test';
import { readStudyRuntime } from './study-runtime.js';

const environment = (): NodeJS.ProcessEnv => ({ GITHUB_REPOSITORY: 'owner/study-public', STUDY_DEFAULT_BRANCH: 'main', STUDY_ACTIONS_TOKEN: 'actions-read-only-test-token' });
const active = (id: number, title: string, status = 'in_progress', branch = 'main') => ({ id, display_title: title, status, head_branch: branch, run_started_at: '2026-09-17T08:50:00Z' });
function response(body: unknown, status = 200): Response { return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }); }
function fetchQueue(responses: Array<Response | Error>): typeof fetch {
  return (async () => {
    const next = responses.shift();
    if (!next) throw new Error('unexpected fetch');
    if (next instanceof Error) throw next;
    return next;
  }) as typeof fetch;
}
const emptyRuns = () => ({ total_count: 0, workflow_runs: [] });

test('distinguishes capture command from an arm still waiting in prepare', async () => {
  const runtime = await readStudyRuntime(environment(), fetchQueue([
    response({ total_count: 2, workflow_runs: [active(1, 'Market study / campaign / early'), active(2, 'Market study / arm / late')] }),
    response(emptyRuns()), response(emptyRuns()), response(emptyRuns()), response(emptyRuns()),
    response({ total_count: 1, jobs: [{ name: 'campaign', status: 'in_progress', steps: [{ name: 'Verify private destination and capture the owned block', status: 'in_progress' }] }] }),
    response({ total_count: 1, jobs: [{ name: 'prepare', status: 'in_progress', steps: [] }] }),
  ]), 0);
  assert.equal(runtime.available, true);
  assert.deepEqual(runtime.runs.map(run => [run.mode, run.captureJobRunning, run.captureCommandRunning, run.preparing]), [
    ['campaign', true, true, false], ['arm', false, false, true],
  ]);
});

test('reports an available empty active set', async () => {
  const runtime = await readStudyRuntime(environment(), fetchQueue([response(emptyRuns()), response(emptyRuns()), response(emptyRuns()), response(emptyRuns()), response(emptyRuns())]), 0);
  assert.deepEqual(runtime, { checkedAt: '1970-01-01T00:00:00.000Z', available: true, reason: null, runs: [] });
});

test('authentication failure is unavailable and does not return remote data', async () => {
  const runtime = await readStudyRuntime(environment(), fetchQueue([response({ message: 'do not expose this', token: 'nope' }, 401)]), 0);
  assert.deepEqual(runtime, { checkedAt: '1970-01-01T00:00:00.000Z', available: false, reason: 'AUTHENTICATION_FAILED', runs: [] });
});

test('deduplicates runs returned by multiple status queries', async () => {
  const run = active(7, 'Market study / observe / early');
  const runtime = await readStudyRuntime(environment(), fetchQueue([
    response({ total_count: 1, workflow_runs: [run] }), response({ total_count: 1, workflow_runs: [run] }), response(emptyRuns()), response(emptyRuns()), response(emptyRuns()),
    response({ total_count: 1, jobs: [{ name: 'utility', status: 'in_progress', steps: [{ name: 'Verify sandbox access or run market smoke with private archive', status: 'in_progress' }] }] }),
  ]), 0);
  assert.equal(runtime.available, true);
  assert.equal(runtime.runs.length, 1);
  assert.equal(runtime.runs[0]?.captureCommandRunning, true);
});

test('filters untrusted branch and title metadata', async () => {
  const runtime = await readStudyRuntime(environment(), fetchQueue([
    response({ total_count: 3, workflow_runs: [active(1, 'Market study / status / early'), active(2, 'Market study / campaign / early', 'in_progress', 'evil'), active(3, 'Market study / campaign / early; injected')] }),
    response(emptyRuns()), response(emptyRuns()), response(emptyRuns()), response(emptyRuns()),
  ]), 0);
  assert.equal(runtime.available, true);
  assert.deepEqual(runtime.runs, []);
});

test('missing credential and truncated run or job pages never report idle', async () => {
  const missing = await readStudyRuntime({ GITHUB_REPOSITORY: 'owner/study-public' }, fetchQueue([]), 0);
  assert.equal(missing.available, false);
  assert.equal(missing.runs.length, 0);
  const truncated = await readStudyRuntime(environment(), fetchQueue([response({ total_count: 101, workflow_runs: Array.from({ length: 100 }, (_, index) => active(index + 1, 'Market study / campaign / early')) })]), 0);
  assert.equal(truncated.available, false);
  assert.equal(truncated.reason, 'TRUNCATED_RESPONSE');
  const jobs = await readStudyRuntime(environment(), fetchQueue([
    response({ total_count: 1, workflow_runs: [active(1, 'Market study / campaign / early')] }), response(emptyRuns()), response(emptyRuns()), response(emptyRuns()), response(emptyRuns()),
    response({ total_count: 101, jobs: Array.from({ length: 100 }, () => ({ name: 'campaign', status: 'queued', steps: [] })) }),
  ]), 0);
  assert.equal(jobs.available, false);
  assert.equal(jobs.reason, 'TRUNCATED_RESPONSE');
});


test('concurrency pending and newly requested runs remain visible as waiting', async () => {
  const queries: string[] = [];
  const fetcher = (async (url: string | URL | Request) => {
    const status = new URL(String(url)).searchParams.get('status')!; queries.push(status);
    const runs = ['pending', 'requested'].includes(status) ? [active(status === 'pending' ? 20 : 21, 'Market study / arm / late', status)] : [];
    return response({ total_count: runs.length, workflow_runs: runs });
  }) as typeof fetch;
  const value = await readStudyRuntime(environment(), fetcher, 0);
  assert.equal(value.available, true); assert.equal(value.runs.length, 2);
  assert.ok(value.runs.every(run => run.queued && !run.captureCommandRunning));
  assert.deepEqual(queries, ['in_progress', 'queued', 'waiting', 'pending', 'requested']);
});
