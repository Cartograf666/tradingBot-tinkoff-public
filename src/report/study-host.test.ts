import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { resolveStudyHost, studyAccessFailure, verifyPrivateStudyTarget } from './study-host.js';

const configured = (): NodeJS.ProcessEnv => ({ GITHUB_REPOSITORY: 'owner/study-public',
  MARKET_STUDY_DATA_REPOSITORY: 'owner/study-data', MARKET_STUDY_STORAGE_TOKEN: 'test-storage-credential',
  TINKOFF_API_TOKEN_SANDBOX: 'test-sandbox-credential', STUDY_HOST_PRIVATE: 'false',
  STUDY_DEFAULT_BRANCH: 'main', GITHUB_REF: 'refs/heads/main', MARKET_STUDY_ENABLED: 'true' });

test('storage access diagnostics distinguish common HTTP failures without echoing upstream data', () => {
  for (const status of [401, 403, 404]) {
    const message = studyAccessFailure(`gh: private response test-secret-do-not-log (HTTP ${status})`);
    assert.match(message, new RegExp(`HTTP ${status}`));
    assert.doesNotMatch(message, /test-secret-do-not-log|private response/);
  }
  assert.equal(studyAccessFailure('unexpected error containing test-secret-do-not-log'),
    'Cannot access the private data repository with MARKET_STUDY_STORAGE_TOKEN');
});

test('public study explicitly targets private storage without returning credential values', () => {
  const env = configured();
  assert.equal(resolveStudyHost(env, 'campaign'), 'owner/study-data');
  assert.doesNotMatch(resolveStudyHost(env, 'campaign'), /credential/);
});

test('same repository, different owner, private host and untrusted ref are rejected before access', () => {
  for (const change of [{ MARKET_STUDY_DATA_REPOSITORY: 'owner/STUDY-PUBLIC' },
    { MARKET_STUDY_DATA_REPOSITORY: 'outsider/study-data' }, { MARKET_STUDY_DATA_REPOSITORY: '../data' },
    { STUDY_HOST_PRIVATE: 'true' }, { GITHUB_REF: 'refs/pull/1/merge' }]) {
    assert.throws(() => resolveStudyHost({ ...configured(), ...change }, 'smoke'));
  }
});

test('public GITHUB_TOKEN and legacy broker token cannot replace the two dedicated credentials', () => {
  assert.throws(() => resolveStudyHost({ ...configured(), MARKET_STUDY_STORAGE_TOKEN: '', GH_TOKEN: 'public-token' }, 'status'), /MARKET_STUDY_STORAGE_TOKEN/);
  assert.throws(() => resolveStudyHost({ ...configured(), TINKOFF_API_TOKEN_SANDBOX: '', TINKOFF_API_TOKEN: 'legacy-token' }, 'smoke'), /TINKOFF_API_TOKEN_SANDBOX/);
  assert.equal(resolveStudyHost({ ...configured(), TINKOFF_API_TOKEN_SANDBOX: '' }, 'status'), 'owner/study-data');
  assert.throws(() => resolveStudyHost({ ...configured(), MARKET_STUDY_ENABLED: 'false' }, 'campaign'), /paused/);
});

test('remote visibility and destination identity must match, while API failure propagates', async () => {
  await verifyPrivateStudyTarget('owner/data', async () => ({ private: true, full_name: 'owner/data' }));
  await assert.rejects(verifyPrivateStudyTarget('owner/data', async () => ({ private: false, full_name: 'owner/data' })), /private/);
  await assert.rejects(verifyPrivateStudyTarget('owner/data', async () => ({ private: true, full_name: 'owner/other' })), /match/);
  await assert.rejects(verifyPrivateStudyTarget('owner/data', async () => { throw new Error('HTTP 403'); }), /403/);
});

test('public workflow keeps all data in private storage and makes code checks independent of secrets', () => {
  const workflow = readFileSync('.github/workflows/market-study.yml', 'utf8');
  assert.doesNotMatch(workflow, /upload-artifact|pull_request_target|--repo "\$GITHUB_REPOSITORY"/);
  assert.match(workflow, /permissions:\n  contents: read/);
  assert.match(workflow, /GH_TOKEN: \$\{\{ secrets\.MARKET_STUDY_STORAGE_TOKEN \}\}/);
  assert.match(workflow, /--repo "\$MARKET_STUDY_DATA_REPOSITORY"/);
  assert.match(workflow, /code-check:[\s\S]*?if:.*inputs\.mode == 'check'/);
  assert.doesNotMatch(workflow.split('  code-check:')[1].split('  campaign:')[0], /secrets\./);
});
