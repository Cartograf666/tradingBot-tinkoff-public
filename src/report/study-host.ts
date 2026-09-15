import { execFile } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export type StudyOperation = 'campaign' | 'preflight' | 'smoke' | 'observe' | 'freeze' | 'status';
class StudyHostError extends Error {}
const repositoryName = /^[A-Za-z0-9][A-Za-z0-9_.-]*\/[A-Za-z0-9][A-Za-z0-9_.-]*$/;

/** Never echo CLI stderr: it can contain upstream response or credential data. */
export function studyAccessFailure(stderr: string): string {
  const status = /\bHTTP (401|403|404)\b/.exec(stderr)?.[1];
  const advice: Record<string, string> = {
    '401': 'GitHub rejected the storage credential; replace it with a valid, unexpired token',
    '403': 'GitHub denied storage access; check token permissions and repository policies',
    '404': 'GitHub cannot expose the private target to this token; check Only select repositories includes the configured PRIVATE repository',
  };
  return status ? `Storage check failed (HTTP ${status}). ${advice[status]}.`
    : 'Cannot access the private data repository with MARKET_STUDY_STORAGE_TOKEN';
}

/** Public compute must explicitly target a DIFFERENT, private data repository.
 * The public repository's automatic GITHUB_TOKEN is never a storage fallback. */
export function resolveStudyHost(environment: NodeJS.ProcessEnv, operation: StudyOperation): string {
  const host = environment.GITHUB_REPOSITORY ?? '';
  const target = environment.MARKET_STUDY_DATA_REPOSITORY ?? '';
  if (!repositoryName.test(host) || !repositoryName.test(target)) throw new StudyHostError('Set MARKET_STUDY_DATA_REPOSITORY to owner/repository');
  if (host.toLowerCase() === target.toLowerCase()) throw new StudyHostError('Market data must remain in a separate private repository');
  if (host.split('/')[0].toLowerCase() !== target.split('/')[0].toLowerCase()) throw new StudyHostError('The data repository must belong to the same owner');
  if (environment.STUDY_HOST_PRIVATE !== 'false') throw new StudyHostError('This workflow requires a public compute repository');
  const defaultBranch = environment.STUDY_DEFAULT_BRANCH;
  if (!defaultBranch || environment.GITHUB_REF !== `refs/heads/${defaultBranch}`) throw new StudyHostError('Study secrets may be used only on the default branch');
  if (!environment.MARKET_STUDY_STORAGE_TOKEN?.trim()) throw new StudyHostError('Set MARKET_STUDY_STORAGE_TOKEN for the private data repository');
  if (operation === 'preflight' || operation === 'smoke' || operation === 'observe' || operation === 'campaign') {
    if (!environment.TINKOFF_API_TOKEN_SANDBOX?.trim()) throw new StudyHostError('Set TINKOFF_API_TOKEN_SANDBOX for sandbox observation');
  }
  if (operation === 'campaign' && environment.MARKET_STUDY_ENABLED !== 'true') throw new StudyHostError('Market study capture is paused');
  return target;
}

export async function verifyPrivateStudyTarget(target: string,
  request: (repository: string) => Promise<{ private?: unknown; full_name?: unknown }>): Promise<void> {
  const remote = await request(target);
  if (remote.private !== true || typeof remote.full_name !== 'string' || remote.full_name.toLowerCase() !== target.toLowerCase()) {
    throw new StudyHostError('The confirmed data repository must be private and match the configured destination');
  }
}

async function main() {
  const operation = process.argv[2];
  if (!['campaign', 'preflight', 'smoke', 'observe', 'freeze', 'status'].includes(operation)) throw new StudyHostError('Unknown study operation');
  const target = resolveStudyHost(process.env, operation as StudyOperation);
  await verifyPrivateStudyTarget(target, repository => new Promise((resolve, reject) => {
    execFile('gh', ['api', `/repos/${repository}`], { encoding: 'utf8', maxBuffer: 1024 * 1024,
      env: { ...process.env, GH_TOKEN: process.env.MARKET_STUDY_STORAGE_TOKEN, GITHUB_TOKEN: '' } }, (error, stdout, stderr) => {
      if (error) { reject(new StudyHostError(studyAccessFailure(stderr))); return; }
      try { resolve(JSON.parse(stdout)); } catch { reject(new StudyHostError('Invalid repository response')); }
    });
  }));
  console.log('Private archive destination verified. No market data will be uploaded to the public repository.');
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => { console.error(error instanceof StudyHostError ? error.message : 'Study destination verification failed'); process.exitCode = 1; });
}
