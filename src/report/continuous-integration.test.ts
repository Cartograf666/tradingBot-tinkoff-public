import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { CONTINUOUS_CAPTURE_POLICY_HASH, materializeStudyInputs } from './market-study.js';
import { STUDY_RELEASE_TAG } from '../research/study-protocol.js';
import type { StudyChunkReceipt } from '../research/study-state.js';
const sha = (data: Buffer) => createHash('sha256').update(data).digest('hex');

test('two scientific references restore one verified raw run, without duplicate replay inputs', async t => {
  const root = mkdtempSync(path.join(tmpdir(), 'continuous-reference-test-'));
  const originalPath = process.env.PATH, originalFixture = process.env.CONTINUOUS_TEST_FIXTURE;
  t.after(() => { process.env.PATH = originalPath;
    if (originalFixture === undefined) delete process.env.CONTINUOUS_TEST_FIXTURE; else process.env.CONTINUOUS_TEST_FIXTURE = originalFixture;
    rmSync(root, { recursive: true, force: true }); });
  const raw = path.join(root, 'raw-run'); mkdirSync(raw);
  const manifest = Buffer.from(JSON.stringify({ runId: 'one-run', recording: { sha256: 'a'.repeat(64) } }));
  writeFileSync(path.join(raw, 'manifest.json'), manifest);
  const makeAsset = (directory: string, name: string, id: number) => {
    const archive = path.join(root, name);
    execFileSync('tar', ['-czf', archive, '-C', root, path.basename(directory)]);
    const bytes = readFileSync(archive);
    return { id, name, size: bytes.length, digest: `sha256:${sha(bytes)}`, archive };
  };
  const sourceAsset = makeAsset(raw, 'continuous-block-one.tar.gz', 100);
  const source = { assetId: 100, assetName: sourceAsset.name, assetBytes: sourceAsset.size,
    assetDigest: sourceAsset.digest, archiveSha256: sourceAsset.digest.slice(7) };
  const identity = { runId: 'one-run', manifestHash: sha(manifest), recordingHash: 'a'.repeat(64) };
  const assets = [sourceAsset], receipts: StudyChunkReceipt[] = [];
  for (let index = 1; index <= 2; index++) {
    const view = path.join(root, `view-${index}`); mkdirSync(view);
    const name = `study-2026-09-17-early-0${index}-1-1.tar.gz`;
    writeFileSync(path.join(view, 'continuous-reference.json'), JSON.stringify({ schemaVersion: 1, kind: 'CONTINUOUS_WINDOW',
      acquisitionPolicyHash: CONTINUOUS_CAPTURE_POLICY_HASH, source, ...identity }));
    writeFileSync(path.join(view, 'study-chunk-receipt.json'), JSON.stringify(identity));
    const asset = makeAsset(view, name, 100 + index); assets.push(asset);
    receipts.push({ ...identity, assetId: asset.id, assetName: name, assetBytes: asset.size,
      assetDigest: asset.digest, archiveSha256: asset.digest.slice(7) } as StudyChunkReceipt);
  }
  const bin = path.join(root, 'bin'); mkdirSync(bin);
  const shim = path.join(bin, 'gh');
  writeFileSync(shim, `#!${process.execPath}\nconst fs = require('node:fs');\nconst f = JSON.parse(fs.readFileSync(process.env.CONTINUOUS_TEST_FIXTURE));\nconst endpoint = process.argv.find(a => a.startsWith('/repos/'));\nif (/\\/releases\\?/.test(endpoint)) process.stdout.write(JSON.stringify([{id: 9, draft:true, tag_name:f.tag}]));\nelse if (/\\/releases\\/9\\/assets\\?/.test(endpoint)) process.stdout.write(JSON.stringify(f.assets));\nelse { const id = Number(endpoint.split('/').at(-1)); const asset = f.assets.find(a => a.id === id); if(!asset) process.exit(2); fs.appendFileSync(f.log, id+'\\n'); process.stdout.write(fs.readFileSync(asset.archive)); }\n`);
  chmodSync(shim, 0o755);
  const fixture = path.join(root, 'fixture.json'), log = path.join(root, 'downloads.log');
  writeFileSync(fixture, JSON.stringify({ tag: STUDY_RELEASE_TAG, assets, log }));
  process.env.PATH = `${bin}${path.delimiter}${originalPath}`; process.env.CONTINUOUS_TEST_FIXTURE = fixture;
  const workspace = path.join(root, 'restore'); mkdirSync(workspace);
  const directories = await materializeStudyInputs('owner/private', receipts, workspace);
  assert.equal(directories.length, 1);
  assert.deepEqual(readFileSync(path.join(directories[0], 'manifest.json')), manifest);
  assert.equal(readFileSync(log, 'utf8').split('\n').filter(line => line === '100').length, 1, 'source downloaded once for both views');
  const bad = { ...receipts[0], manifestHash: 'b'.repeat(64) };
  const other = path.join(root, 'bad'); mkdirSync(other);
  await assert.rejects(materializeStudyInputs('owner/private', [bad], other), /identity differs/);
});
