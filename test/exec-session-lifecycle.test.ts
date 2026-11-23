import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, expect, it, vi } from 'vitest';
import {
  COMPLETED_EXEC_OUTPUT_BYTES,
  COMPLETED_EXEC_RETENTION_MS,
  MAX_COMPLETED_EXEC_RESULTS,
  UnifiedExecProcessManager,
  applyUnifiedExecEnv
} from '../src/main/codex/unified-exec.js';
import { INTERRUPT, MAX_UNIFIED_EXEC_PROCESSES } from '../src/main/codex/unified-exec-constants.js';

const managers: UnifiedExecProcessManager[] = [];
const truncationPolicy = { kind: 'tokens' as const, tokens: 10_000 };
afterEach(async () => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  await Promise.all(managers.splice(0).map(manager => manager.terminateAllProcesses()));
});

function makeManager() {
  const manager = new UnifiedExecProcessManager(5_000);
  managers.push(manager);
  return manager;
}

function start(manager: UnifiedExecProcessManager, script: string, options: {
  tty?: boolean;
  file?: string;
  processId?: number;
} = {}) {
  return manager.execCommand({
    command: [options.file ?? process.execPath, '-e', script],
    shellType: 'bash',
    hookCommand: 'terminal lifecycle fixture',
    processId: options.processId ?? manager.allocateProcessId(),
    yieldTimeMs: 250, maxOutputTokens: undefined, truncationPolicy,
    cwd: process.cwd(), displayCwd: '.', env: applyUnifiedExecEnv(process.env),
    tty: options.tty ?? false
  });
}

function poll(manager: UnifiedExecProcessManager, processId: number, input = '') {
  return manager.writeStdin({ processId, input, yieldTimeMs: 250, maxOutputTokens: undefined, truncationPolicy });
}

it('retires 104 yielded commands across eight concurrent callers without a single drain', async () => {
  const manager = makeManager();
  const released = vi.fn();
  manager.setProcessReleaseListener(released);
  const finished = await Promise.all(Array.from({ length: 8 }, async (_, caller) => {
    let lastId = 0;
    for (let run = 0; run < 13; run++) {
      const initial = await start(manager, `setTimeout(() => console.log('caller-${caller}-${run}'), 575)`);
      expect(initial.processId).not.toBeNull();
      lastId = initial.processId!;
      await expect(initial.completion).resolves.toMatchObject({ exitCode: 0 });
    }
    return lastId;
  }));
  expect(manager.listProcesses()).toEqual([]);
  expect(released).toHaveBeenCalledTimes(104 - MAX_COMPLETED_EXEC_RESULTS);
  expect(new Set(released.mock.calls.map(([id]) => id)).size).toBe(104 - MAX_COMPLETED_EXEC_RESULTS);
  for (const [caller, id] of finished.entries()) {
    await expect(poll(manager, id)).resolves.toMatchObject({
      completedSessionId: id, rawOutput: Buffer.from(`caller-${caller}-12\n`)
    });
  }
  await expect(start(manager, '')).resolves.toMatchObject({ exitCode: 0 });
});

it('recovers a full table of completed entries on admission and leaves an active PTY alone', async () => {
  const manager = makeManager();
  const interactive = await start(manager, `process.stdin.setEncoding('utf8'); process.stdin.resume();
    process.stdin.on('data', input => { if (input.includes('finish')) {
      console.log('interactive-alive'); process.stdin.pause();
    } });`, { tty: true });
  const interactiveId = interactive.processId!;
  expect(interactiveId).not.toBeNull();

  // Model the legacy in-memory state by withholding automatic retirement. Admission must
  // recover even when the completed entries predate the exit listener.
  const retirement = vi.spyOn(manager as unknown as { retireIfIdle: () => void }, 'retireIfIdle').mockImplementation(() => {});
  const reserve = (manager as unknown as { reservedProcessIds: Set<number> }).reservedProcessIds;
  const doneIds: number[] = [];
  for (let startAt = 1; startAt < MAX_UNIFIED_EXEC_PROCESSES; startAt += 8) {
    const batch = await Promise.all(Array.from({ length: Math.min(8, MAX_UNIFIED_EXEC_PROCESSES - startAt) }, () =>
      start(manager, "setTimeout(() => console.log('old-output'), 575)")));
    for (const output of batch) {
      expect(output.processId).not.toBeNull();
      doneIds.push(output.processId!);
      await output.completion;
    }
  }
  expect(reserve.size).toBe(MAX_UNIFIED_EXEC_PROCESSES);
  retirement.mockRestore();
  await expect(start(manager, "console.log('recovered')")).resolves.toMatchObject({
    exitCode: 0, rawOutput: Buffer.from('recovered\n')
  });
  expect([...reserve]).toEqual([interactiveId]);
  expect(manager.listProcesses().map(entry => entry.processId)).toEqual([interactiveId]);
  await expect(poll(manager, doneIds[0]!)).resolves.toMatchObject({ rawOutput: Buffer.from('old-output\n') });
  const response = await poll(manager, interactiveId, 'finish\r');
  expect(response.rawOutput.toString()).toContain('interactive-alive');
});

it('expires unread output automatically, bounds its bytes, and disposes custody once', async () => {
  const manager = makeManager();
  const released = vi.fn();
  manager.setProcessReleaseListener(released);
  const now = vi.spyOn(performance, 'now');
  // Use a real child for exit and a fixed monotonic clock for the expiry deadline.
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
  now.mockReturnValue(10);
  const execution = start(manager, `setTimeout(() => process.stdout.write('a'.repeat(${COMPLETED_EXEC_OUTPUT_BYTES * 2})+'TAIL'), 950)`);
  await vi.advanceTimersByTimeAsync(500);
  // If the OS has not scheduled the child yet, the early-exit timer may only now be created.
  await vi.advanceTimersByTimeAsync(500);
  const initial = await execution;
  expect(initial.processId).not.toBeNull();
  await initial.completion;
  const id = initial.processId!;
  const page = await manager.offerCompletedOutput(new Set([id]), { completedAt: null, failed: false }, 4096);
  expect(page?.total).toBeLessThan(COMPLETED_EXEC_OUTPUT_BYTES + 200);
  expect(page?.total).toBeGreaterThanOrEqual(COMPLETED_EXEC_OUTPUT_BYTES);
  now.mockReturnValue(10 + COMPLETED_EXEC_RETENTION_MS);
  await vi.advanceTimersByTimeAsync(COMPLETED_EXEC_RETENTION_MS + 1);
  await expect(poll(manager, id)).rejects.toThrow('Unknown process id');
  await Promise.all([manager.terminateAllProcesses(), manager.terminateAllProcesses()]);
  expect(released.mock.calls).toEqual([[id]]);
});

it('retains failures and interrupted exits while releasing failed launches and wait deadlines', async () => {
  const manager = makeManager();
  for (let run = 0; run < MAX_UNIFIED_EXEC_PROCESSES + 1; run++) {
    await expect(start(manager, '', { file: join(tmpdir(), 'cos-nonexistent-lifecycle-fixture', 'no-executable') }))
      .rejects.toMatchObject({ kind: 'create_process' });
  }
  const initial = await start(manager, "process.on('SIGINT', () => { console.log('interrupted'); process.exit(19); }); setInterval(() => {}, 1000)");
  const id = initial.processId!;
  expect(id).not.toBeNull();
  // An empty poll's wait deadline yields the same active session, with capacity still charged.
  await expect(poll(manager, id)).resolves.toMatchObject({ processId: id });
  await poll(manager, id, INTERRUPT);
  const final = await poll(manager, id);
  expect(final.processId).toBeNull();
  if (process.platform !== 'win32') expect(final).toMatchObject({ exitCode: 19, rawOutput: Buffer.from('interrupted\n') });
  await expect(start(manager, 'process.exit(7)')).resolves.toMatchObject({ exitCode: 7 });
});

it('settles after the leader exits even when its descendant is still using stdout', async () => {
  const manager = makeManager();
  const dir = await mkdtemp(join(tmpdir(), 'cos-terminal-inherited-output-'));
  const marker = join(dir, 'still-running');
  const descendant = `setTimeout(() => { process.stdout.write('late\\n'); require('node:fs').writeFileSync(${JSON.stringify(marker)}, 'survived'); }, 1200)`;
  const script = `const child = require('node:child_process').spawn(process.execPath, ['-e', ${JSON.stringify(descendant)}],
    { stdio: ['ignore', 1, 2] }); child.unref(); setTimeout(() => { console.log('leader'); process.exit(0); }, 575)`;
  try {
    const initial = await start(manager, script);
    expect(initial.processId).not.toBeNull();
    await initial.completion;
    await expect(poll(manager, initial.processId!)).resolves.toMatchObject({ rawOutput: Buffer.from('leader\n') });
    await expect.poll(() => readFile(marker, 'utf8').catch(() => ''), { timeout: 5_000 }).toBe('survived');
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

it('handles concurrent shutdown during spawn, and a new app starts with fresh custody', async () => {
  const manager = makeManager();
  const release = vi.fn();
  manager.setProcessReleaseListener(release);
  const cached = await start(manager, "console.log('before-shutdown')");
  const live = await start(manager, 'setInterval(() => {}, 1000)');
  const pendingId = manager.allocateProcessId();
  const interrupted = expect(start(manager, 'setInterval(() => {}, 1000)', { processId: pendingId }))
    .rejects.toThrow('shut down during launch');
  await Promise.all([interrupted, manager.terminateAllProcesses(), manager.terminateAllProcesses()]);
  expect(manager.listProcesses()).toEqual([]);
  expect(release.mock.calls.map(([id]) => id).sort()).toEqual(
    [cached.completedSessionId, live.processId, pendingId].sort()
  );
  const restarted = makeManager();
  await expect(poll(restarted, cached.completedSessionId!)).rejects.toThrow('Unknown process id');
  await expect(start(restarted, "console.log('after-restart')")).resolves.toMatchObject({ exitCode: 0 });
});
