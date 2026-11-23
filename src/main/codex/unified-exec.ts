/**
 * Codex "unified exec": the runtime behind `exec_command` and `write_stdin`.
 *
 * Ported from `codex-rs/core/src/unified_exec/` — `mod.rs`, `process.rs`, `process_manager.rs`
 * and `errors.rs` — together with `ExecCommandToolOutput` from `codex-rs/core/src/tools/context.rs`.
 *
 * Nothing here shells out to Codex or depends on a Codex install. The Rust tokio machinery
 * (`Notify`, `CancellationToken`, `watch`, an interaction `Mutex`) is reproduced with the
 * JavaScript equivalents so the observable semantics survive: one shared output buffer that
 * a poll *drains*, a head/tail cap on what is retained, a yield deadline with a short
 * post-exit grace, and a session that outlives the call that created it.
 *
 * Two Windows adaptations, both about launching rather than about behaviour:
 *   - `cmd.exe` gets a verbatim command line. Node (like Rust) would otherwise escape inner
 *     quotes as `\"`, which cmd has never understood, and cmd exits 0 after failing to run
 *     it — a command that silently does nothing.
 *   - `interrupt()` terminates Windows pipe process trees; POSIX uses group SIGINT. A Windows console
 *     control event cannot be delivered to a child that owns no console.
 */

import { randomInt } from 'node:crypto';
import { spawn, type ChildProcess } from 'node:child_process';
import { HeadTailBuffer } from './head-tail-buffer.js';
import { CommandBatchDisplay } from './command-batch.js';
import {
  approxTokenCount,
  approxTokensFromByteCount,
  byteLength,
  formattedTruncateText,
  policyTokenBudget,
  truncateText,
  type TruncationPolicy
} from './truncate.js';
import {
  clampYieldTime,
  DEFAULT_TERMINAL_COLS,
  DEFAULT_TERMINAL_ROWS,
  formatOutputOmissionMarker,
  generateChunkId,
  INTERRUPT,
  MAX_UNIFIED_EXEC_PROCESSES,
  MAX_YIELD_TIME_MS,
  MIN_EMPTY_YIELD_TIME_MS,
  MIN_YIELD_TIME_MS,
  resolveMaxTokens,
  UNIFIED_EXEC_ENV
} from './unified-exec-constants.js';
import { terminateProcessTree } from '../exec.js';
import { prefixPowershellScriptWithUtf8, withPowershellExitStatus, type ShellType } from './shell.js';

// --------------------------------------------------------------------------- errors


/**
 * An exit status as the signed 32-bit number Codex reports (Rust's `ExitStatus::code()` is an
 * i32). Windows hands a process's DWORD over unsigned, so a program exiting with -1 (ssh.exe on a
 * dropped connection) arrived as 4294967295.
 */
export function signedExitCode(code: number | null): number | null {
  return code !== null && code > 0x7fffffff && code <= 0xffffffff ? code - 0x100000000 : code;
}

export type UnifiedExecErrorKind =
  | 'create_process'
  | 'process_failed'
  | 'unknown_process_id'
  | 'write_to_stdin'
  | 'stdin_closed'
  | 'missing_command_line';

/**
 * `UnifiedExecError`, with both of the Rust renderings it is shown through.
 *
 * `message` is the `Display` form, which is what `write_stdin failed: {err}` prints; `debug()`
 * is the derived `Debug` form, which is what `exec_command failed for ...: {err:?}` prints.
 * They differ, and both reach the model, so both are reproduced.
 */
export class UnifiedExecError extends Error {
  readonly kind: UnifiedExecErrorKind;
  readonly processId: number | null;
  readonly detail: string | null;

  private constructor(kind: UnifiedExecErrorKind, message: string, detail: string | null, processId: number | null) {
    super(message);
    this.name = 'UnifiedExecError';
    this.kind = kind;
    this.detail = detail;
    this.processId = processId;
  }

  static createProcess(message: string): UnifiedExecError {
    return new UnifiedExecError('create_process', `Failed to create unified exec process: ${message}`, message, null);
  }

  static processFailed(message: string): UnifiedExecError {
    return new UnifiedExecError('process_failed', `Unified exec process failed: ${message}`, message, null);
  }

  static unknownProcessId(processId: number): UnifiedExecError {
    return new UnifiedExecError('unknown_process_id', `Unknown process id ${processId}`, null, processId);
  }

  static writeToStdin(): UnifiedExecError {
    return new UnifiedExecError('write_to_stdin', 'failed to write to stdin', null, null);
  }

  static stdinClosed(): UnifiedExecError {
    return new UnifiedExecError(
      'stdin_closed',
      'stdin is closed for this session; rerun exec_command with tty=true to keep stdin open',
      null,
      null
    );
  }

  static missingCommandLine(): UnifiedExecError {
    return new UnifiedExecError('missing_command_line', 'missing command line for unified exec request', null, null);
  }

  /** The `{err:?}` rendering of the Rust enum. */
  debug(): string {
    switch (this.kind) {
      case 'create_process':
        return `CreateProcess { message: ${JSON.stringify(this.detail ?? '')} }`;
      case 'process_failed':
        return `ProcessFailed { message: ${JSON.stringify(this.detail ?? '')} }`;
      case 'unknown_process_id':
        return `UnknownProcessId { process_id: ${this.processId ?? 0} }`;
      case 'write_to_stdin':
        return 'WriteToStdin';
      case 'stdin_closed':
        return 'StdinClosed';
      case 'missing_command_line':
        return 'MissingCommandLine';
    }
  }
}

// --------------------------------------------------------------------------- notify

/** `tokio::sync::Notify`, minus the stored permit: waiters registered before the notify. */
class Notify {
  private waiters = new Set<() => void>();

  /** A one-shot wait that must be disposed, so a lost race does not leak its resolver. */
  notified(): { promise: Promise<void>; dispose: () => void } {
    let resolver: () => void = () => {};
    const promise = new Promise<void>((resolve) => {
      resolver = resolve;
      this.waiters.add(resolver);
    });
    return { promise, dispose: () => this.waiters.delete(resolver) };
  }

  notifyWaiters(): void {
    const waiters = [...this.waiters];
    this.waiters.clear();
    for (const resolve of waiters) resolve();
  }
}

/** An async mutex standing in for the per-process `interaction_lock`. */
class Mutex {
  private tail: Promise<void> = Promise.resolve();
  private locked = false;
  private queued = 0;

  async lock(): Promise<() => void> {
    // Count the request before the first await. A holder releases synchronously, while the next
    // queued lock only resumes in a microtask; without this bit of state tryLock() can barge into
    // that handoff gap even though a waiter is already entitled to the lock.
    this.queued += 1;
    let release: () => void = () => {};
    const next = new Promise<void>((resolve) => {
      release = () => {
        this.locked = false;
        resolve();
      };
    });
    const previous = this.tail;
    this.tail = previous.then(() => next);
    await previous;
    this.queued -= 1;
    this.locked = true;
    return release;
  }

  /** `try_lock_owned`: null when someone else holds it, so pruning can skip busy sessions. */
  tryLock(): (() => void) | null {
    if (this.locked || this.queued > 0) return null;
    let release: () => void = () => {};
    const next = new Promise<void>((resolve) => {
      release = () => {
        this.locked = false;
        resolve();
      };
    });
    this.tail = this.tail.then(() => next);
    this.locked = true;
    return release;
  }
}

// --------------------------------------------------------------------------- pty loading

/** The slice of node-pty this module uses, declared locally so an absent module is not a build edge. */
interface PtyProcess {
  readonly pid: number;
  onData(listener: (data: string) => void): unknown;
  onExit(listener: (event: { exitCode: number; signal?: number }) => void): unknown;
  write(data: string): void;
  resize(cols: number, rows: number): void;
  kill(signal?: string): void;
}

interface PtyModule {
  spawn(
    file: string,
    args: readonly string[] | string,
    options: { name: string; cols: number; rows: number; cwd: string; env: Record<string, string> }
  ): PtyProcess;
}

let ptyModule: Promise<PtyModule | null> | null = null;

async function loadPty(): Promise<PtyModule | null> {
  ptyModule ??= import('node-pty')
    .then((module) => ((module as { default?: PtyModule }).default ?? module) as PtyModule)
    .catch(() => null);
  return ptyModule;
}

// --------------------------------------------------------------------------- process

export interface SpawnParams {
  batchMarker?: string;
  /** The derived argv; element 0 is the executable. */
  command: string[];
  shellType: ShellType;
  cwd: string;
  env: NodeJS.ProcessEnv;
  tty: boolean;
}

const EARLY_EXIT_GRACE_PERIOD_MS = 150;
const POST_EXIT_CLOSE_WAIT_CAP_MS = 50;
/**
 * A person's view of a command (#1920) shows its exit line only after its output went quiet this
 * long: a PTY closes its output with its exit, yet its last data can arrive just after it
 * (CodeRabbit on #2052). Bounded, and only the view waits; the agent's own reads are unchanged.
 */
const WATCH_EXIT_QUIET_MS = 150;
const WATCH_EXIT_QUIET_CAP_MS = 1_000;
export const MAX_COMPLETED_EXEC_RESULTS = 64;
export const COMPLETED_EXEC_OUTPUT_BYTES = 256 * 1024;
export const COMPLETED_EXEC_RETENTION_MS = 15 * 60_000;

/** Applies `UNIFIED_EXEC_ENV` over the caller's environment, as `apply_unified_exec_env`. */
export function applyUnifiedExecEnv(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const result: NodeJS.ProcessEnv = { ...env };
  for (const [key, value] of UNIFIED_EXEC_ENV) result[key] = value;
  return result;
}

function stringEnv(env: NodeJS.ProcessEnv): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(env)) if (value !== undefined) out[key] = value;
  return out;
}

/** MSVCRT-style quoting for one argument, for the verbatim `cmd.exe` command line only. */
function quoteWindowsArgument(argument: string): string {
  if (argument !== '' && !/[\s"]/.test(argument)) return argument;
  return `"${argument.replace(/(\\*)"/g, '$1$1\\"').replace(/(\\*)$/, '$1$1')}"`;
}

/**
 * One running unified exec session.
 *
 * The output buffer is *drained* by whoever polls it, which is the property the whole design
 * rests on: a chunk is delivered to exactly one call, so two consecutive `write_stdin` polls
 * see new bytes while work runs. Once completed delivery is acknowledged, the manager's
 * bounded history permits explicitly labeled rereads without executing the command again.
 */
class UnifiedExecProcess {
  readonly identity = {};
  private readonly startedAt = Date.now();
  private resolveCompletion!: (value: ProcessCompletion) => void;
  readonly completion = new Promise<ProcessCompletion>(resolve => { this.resolveCompletion = resolve; });
  readonly outputSettled = this.completion.then(async () => {
    if (!this.outputClosed) {
      const closed = this.outputClosedNotify.notified();
      try { this.outputTimedOut = await raceWithTimeout([closed], POST_EXIT_CLOSE_WAIT_CAP_MS); }
      finally { closed.dispose(); }
      this.closeOutput();
    }
    // Descendants can inherit pipes and outlive the leader. Stop recording after the
    // drain window while leaving their pipes flowing; never signal them from cleanup.
    this.detachPipeCapture?.();
    this.detachPipeCapture = undefined;
  });
  private buffer = new HeadTailBuffer();
  private history = new HeadTailBuffer(COMPLETED_EXEC_OUTPUT_BYTES);
  private recording = true;
  private outputTimedOut = false;
  private detachPipeCapture?: () => void;
  private displayBuffer: HeadTailBuffer | undefined;
  private readonly batchDisplay: CommandBatchDisplay | undefined;
  readonly outputNotify = new Notify();
  readonly outputClosedNotify = new Notify();
  readonly cancelNotify = new Notify();
  readonly interactionLock = new Mutex();
  outputClosed = false;
  cancelled = false;
  private exited = false;
  private exit: number | null = null;
  private failure: string | null = null;
  private classifiedExit?: boolean;
  private openStreams = 0;
  private child: ChildProcess | null = null;
  private pty: PtyProcess | null = null;
  readonly tty: boolean;
  private readonly spawnPid: number;

  private constructor(tty: boolean, pid: number, batchMarker?: string) {
    this.tty = tty;
    this.spawnPid = pid;
    if (batchMarker) {
      this.batchDisplay = new CommandBatchDisplay(batchMarker);
      this.displayBuffer = new HeadTailBuffer();
    }
  }

  /**
   * The session leader's OS pid, read through to the live handle rather than snapshotted.
   *
   * node-pty's Windows ConPTY backend connects asynchronously: `pty.spawn()` hands back a
   * handle whose `pid` is still 0, and it is filled in later, when the conout pipe reports
   * `ready_datapipe` — comfortably after `EARLY_EXIT_GRACE_PERIOD_MS`. Recording it at
   * construction therefore stored 0 for every Windows tty session for the session's whole
   * life. That made `list_processes` advertise a pid nobody can act on, and, worse, made
   * `terminate()` fail its own `pid > 0` test and skip `terminateProcessTree` entirely — so
   * whenever node-pty had deferred its internal `kill()` (it queues the call until the pty is
   * ready), nothing ever killed the shell, and its console host outlived the app. Every
   * consumer reads this well after connect, so reading through is both correct and enough.
   */
  get pid(): number {
    const live = this.pty?.pid ?? 0;
    return live > 0 ? live : this.spawnPid;
  }

  static async spawn(params: SpawnParams): Promise<UnifiedExecProcess> {
    const file = params.command[0];
    if (file === undefined || params.command.length === 0) throw UnifiedExecError.missingCommandLine();
    const args = params.command.slice(1);

    if (params.tty) {
      const pty = await loadPty();
      if (!pty) {
        throw UnifiedExecError.createProcess('a pseudo-console is not available on this machine');
      }
      let handle: PtyProcess;
      try {
        handle = pty.spawn(
          file,
          // cmd.exe needs the command line it will actually parse; everything else is
          // quoted by node-pty from the argument list.
          params.shellType === 'cmd' && process.platform === 'win32'
            ? [quoteWindowsArgument(file), ...args].join(' ')
            : args,
          {
            name: 'dumb',
            cols: DEFAULT_TERMINAL_COLS,
            rows: DEFAULT_TERMINAL_ROWS,
            cwd: params.cwd,
            env: stringEnv(params.env)
          }
        );
      } catch (error) {
        throw UnifiedExecError.createProcess(error instanceof Error ? error.message : String(error));
      }
      const managed = new UnifiedExecProcess(true, handle.pid, params.batchMarker);
      managed.pty = handle;
      handle.onData((data) => managed.pushChunk(Buffer.from(data, 'utf8')));
      handle.onExit((event) => {
        managed.signalExit(event.exitCode);
        managed.closeOutput();
      });
      await managed.waitForEarlyExit();
      return managed;
    }

    let child: ChildProcess;
    try {
      const verbatim = params.shellType === 'cmd' && process.platform === 'win32';
      child = spawn(file, args, {
        cwd: params.cwd,
        env: params.env,
        windowsHide: true,
        shell: false,
        // Codex sessions must own descendants as well as the shell process. On POSIX a
        // detached child is a process-group leader, which lets interrupt/terminate signal
        // the whole session without changing Windows' existing taskkill semantics.
        detached: process.platform !== 'win32',
        // `stdin_open: tty` in Codex: a pipe session has no stdin, which is what makes
        // write_stdin answer StdinClosed rather than pretending the write landed.
        stdio: ['ignore', 'pipe', 'pipe'],
        ...(verbatim ? { windowsVerbatimArguments: true } : {})
      });
    } catch (error) {
      throw UnifiedExecError.createProcess(error instanceof Error ? error.message : String(error));
    }

    const managed = new UnifiedExecProcess(false, child.pid ?? -1, params.batchMarker);
    managed.child = child;
    // stdout and stderr are combined into one stream, exactly as `combine_output_receivers`
    // does on the local Codex path, so interleaving is preserved in arrival order.
    let capture: UnifiedExecProcess | null = managed;
    managed.detachPipeCapture = () => { capture = null; };
    for (const stream of [child.stdout, child.stderr]) {
      if (!stream) continue;
      managed.openStreams += 1;
      stream.on('data', (chunk: Buffer) => capture?.pushChunk(chunk));
      stream.on('close', () => capture?.streamEnded());
      stream.on('error', () => {});
    }
    if (managed.openStreams === 0) managed.closeOutput();

    child.once('error', (error: Error) => {
      if (!capture) return;
      capture.failure = error.message;
      capture.signalExit(null);
      capture.closeOutput();
    });
    child.once('exit', (code) => capture?.signalExit(code));

    await managed.waitForEarlyExit();
    // Node reports an OS-level spawn failure (for example ENOENT for a missing
    // executable) through the ChildProcess `error` event instead of throwing
    // from spawn(). Codex's spawn_process returns that same failure from the
    // creation boundary, so it is a CreateProcess error rather than a later
    // ProcessFailed error. The event necessarily arrives before a successful
    // `spawn` event, well inside Codex's 150 ms early-exit grace window.
    if (managed.failure !== null && child.pid === undefined) {
      throw UnifiedExecError.createProcess(managed.failure);
    }
    return managed;
  }

  /** `EARLY_EXIT_GRACE_PERIOD`: a command that is already over must not be stored as live. */
  private async waitForEarlyExit(): Promise<void> {
    if (this.cancelled) return;
    const wait = this.cancelNotify.notified();
    try {
      await Promise.race([wait.promise, sleep(EARLY_EXIT_GRACE_PERIOD_MS)]);
    } finally {
      wait.dispose();
    }
  }

  /** Read-only viewers of this output (#1920); they see each chunk and take nothing from the agent's reads. */
  private readonly watchers = new Set<(chunk: Buffer) => void>();
  watch(listener: (chunk: Buffer) => void): () => void { this.watchers.add(listener); return () => { this.watchers.delete(listener); }; }
  /** The output so far, as retained for completed results (head and tail, with a marker where bytes were left out). */
  historyBytes(): Buffer { return this.history.toBytesWithOmissionMarker(); }

  private pushChunk(chunk: Buffer): void {
    if (chunk.length === 0) return;
    for (const watcher of this.watchers) { try { watcher(chunk); } catch { /* a viewer never breaks the command */ } }
    if (!this.recording) return;
    this.history.pushChunk(chunk);
    this.buffer.pushChunk(chunk);
    if (this.batchDisplay) this.displayBuffer!.pushChunk(this.batchDisplay.push(chunk));
    this.outputNotify.notifyWaiters();
  }

  private streamEnded(): void {
    this.openStreams -= 1;
    if (this.openStreams <= 0) this.closeOutput();
  }

  private closeOutput(): void {
    if (this.outputClosed) return;
    if (this.batchDisplay) this.displayBuffer!.pushChunk(this.batchDisplay.push(Buffer.alloc(0), true));
    this.outputClosed = true;
    this.outputClosedNotify.notifyWaiters();
  }

  private signalExit(raw: number | null): void {
    const exitCode = signedExitCode(raw);
    if (!this.exited) {
      this.exited = true;
      this.exit = exitCode;
      const completedAt = Date.now();
      this.resolveCompletion({ exitCode, completedAt, durationMs: Math.max(0, completedAt - this.startedAt) });
    }
    if (!this.cancelled) {
      this.cancelled = true;
      this.cancelNotify.notifyWaiters();
    }
  }

  /** `std::mem::take` of the shared buffer. */
  takeBuffer(): { raw: HeadTailBuffer; display?: HeadTailBuffer } {
    const drained = { raw: this.buffer, display: this.displayBuffer };
    this.buffer = new HeadTailBuffer();
    if (this.displayBuffer) this.displayBuffer = new HeadTailBuffer();
    return drained;
  }

  discardCapturedOutput(): void {
    this.recording = false;
    this.history = new HeadTailBuffer(COMPLETED_EXEC_OUTPUT_BYTES);
  }

  /** Independent of the unread cursor: explicit rereads include earlier delivered chunks. */
  retainedOutput(): Buffer {
    return this.history.toBytesWithOmissionMarker();
  }

  benignExit(classify: ExecCommandRequest['classifyExit']): boolean {
    // An omitted diagnostic or still-open stream cannot prove a non-zero result benign.
    if (!this.hasExited() || !this.outputClosed) return false;
    return this.classifiedExit ??= !this.outputTimedOut && this.history.omittedBytes() === 0 &&
      (classify?.(this.exitCode(), this.retainedOutput().toString('utf8')) ?? false);
  }

  hasExited(): boolean {
    return this.exited;
  }

  exitCode(): number | null {
    return this.exit;
  }

  failureMessage(): string | null {
    return this.failure;
  }

  async write(data: string): Promise<void> {
    if (this.pty) {
      try {
        this.pty.write(data);
        return;
      } catch {
        throw UnifiedExecError.writeToStdin();
      }
    }
    // A pipe session was spawned without stdin, so there is nothing to write to. Codex
    // reaches the same answer through `StdinClosed` before ever calling write.
    throw UnifiedExecError.writeToStdin();
  }

  async interrupt(): Promise<void> {
    if (this.pty) {
      try {
        this.pty.write(INTERRUPT);
        return;
      } catch (error) {
        throw UnifiedExecError.processFailed(error instanceof Error ? error.message : String(error));
      }
    }
    const pid = this.child?.pid;
    if (pid === undefined) throw UnifiedExecError.processFailed('the process is no longer running');
    try {
      // Windows pipe children have no console for Ctrl-C. Terminate the tree before
      // its leader disappears, otherwise descendants retain the session's pipes/cwd.
      if (process.platform === 'win32') await terminateProcessTree(pid, true);
      else process.kill(-pid, 'SIGINT');
    } catch (error) {
      // The managed session can outlive the OS process for the tiny window before Node's
      // ChildProcess `exit` event reaches us. Ctrl+C in that window used to turn a successful
      // natural exit into `kill ESRCH`, while the next empty poll immediately returned DONE.
      // "Already gone" is exactly the terminal state the interrupt was trying to reach.
      if ((error as NodeJS.ErrnoException).code === 'ESRCH') return;
      throw UnifiedExecError.processFailed(error instanceof Error ? error.message : String(error));
    }
  }

  async terminate(): Promise<void> {
    if (this.pty) {
      try {
        this.pty.kill();
      } catch {
        /* already gone */
      }
      // node-pty queues `kill()` until the pty reports ready and drops it silently if that
      // never happens, so the process tree is the authority here, not the handle. `pid` is a
      // live read for exactly this reason.
      if (this.pid > 0) await terminateProcessTree(this.pid, true);
    } else if (this.child?.pid !== undefined) {
      await terminateProcessTree(this.child.pid, true);
    }
    this.signalExit(this.exit);
    this.closeOutput();
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, Math.max(0, ms));
    if (typeof timer.unref === 'function') timer.unref();
  });
}

// --------------------------------------------------------------------------- tool output

export interface ProcessCompletion {
  benignExit?: boolean;
  exitCode: number | null;
  completedAt: number;
  durationMs: number;
}

export interface ExecCommandToolOutput {
  completedSessionId?: number;
  replayed?: boolean;
  benignExit?: boolean;
  /** Exact process lifetime for recording; never serialized into the MCP response. */
  completion?: Promise<ProcessCompletion>;
  /** Exact lifetime for registering ownership after the initial response has been collected. */
  identity?: object;
  chunkId: string;
  wallTimeMs: number;
  rawOutput: Buffer;
  /** Batches alone retain a separately bounded, delimiter-free presentation stream. */
  displayOutput?: Buffer;
  truncationPolicy: TruncationPolicy;
  maxOutputTokens: number | undefined;
  /** The session id, present only while the process is still running. */
  processId: number | null;
  exitCode: number | null;
  originalTokenCount: number | null;
  /** Bytes the 1 MiB collection cap dropped, before model-facing truncation. */
  outputOmittedBytes: number | null;
}

function modelOutputMaxTokens(output: ExecCommandToolOutput): number {
  return Math.min(resolveMaxTokens(output.maxOutputTokens), policyTokenBudget(output.truncationPolicy));
}

/** `ExecCommandToolOutput::truncated_output`. */
export function truncatedOutput(output: ExecCommandToolOutput, maxTokens: number): string {
  const text = (output.displayOutput ?? output.rawOutput).toString('utf8');
  const policy: TruncationPolicy = { kind: 'tokens', tokens: maxTokens };
  if (output.outputOmittedBytes === null || output.outputOmittedBytes === 0) {
    return formattedTruncateText(text, policy);
  }

  const marker = formatOutputOmissionMarker(output.outputOmittedBytes);
  if (byteLength(text) <= maxTokens * 4) {
    return text.includes(marker) ? text : `${marker}\n${text}`;
  }

  const originalTokenCount = output.originalTokenCount ?? approxTokenCount(text);
  const truncated = truncateText(text, policy);
  const omissionNotice = truncated.includes(marker) ? '' : `${marker}\n`;
  return `Warning: truncated output (original token count: ${originalTokenCount})\n${omissionNotice}\n${truncated}`;
}

/** `ExecCommandToolOutput::response_text` — exactly what the model is handed. */
export function execCommandResponseText(output: ExecCommandToolOutput): string {
  const sections: string[] = [];
  if (output.chunkId !== '') sections.push(`Chunk ID: ${output.chunkId}`);
  sections.push(`Wall time: ${(output.wallTimeMs / 1000).toFixed(4)} seconds`);
  if (output.exitCode !== null) sections.push(`Process exited with code ${output.exitCode}`);
  if (output.processId !== null) sections.push(`Process running with session ID ${output.processId}`);
  if (output.completedSessionId !== undefined) sections.push(`Completed session ID: ${output.completedSessionId}`);
  if (output.replayed) sections.push('Retained output (already completed; command was not run again):');
  if (output.benignExit) sections.push('This non-zero exit is an expected command result, not a failure.');
  if (output.originalTokenCount !== null) sections.push(`Original token count: ${output.originalTokenCount}`);
  sections.push('Output:');
  sections.push(truncatedOutput(output, modelOutputMaxTokens(output)));
  return sections.join('\n');
}

/** `ExecCommandToolOutput::code_mode_result`, which is also the tool's declared output schema. */
export function execCommandStructuredOutput(output: ExecCommandToolOutput): Record<string, unknown> {
  return {
    ...(output.chunkId === '' ? {} : { chunk_id: output.chunkId }),
    wall_time_seconds: output.wallTimeMs / 1000,
    ...(output.exitCode === null ? {} : { exit_code: output.exitCode }),
    ...(output.processId === null ? {} : { session_id: output.processId }),
    ...(output.completedSessionId === undefined ? {} : { completed_session_id: output.completedSessionId }),
    // Named for what it says (#1417): "benign" read to ChatGPT's reviewer as a verdict on risk.
    ...(output.benignExit ? { expected_exit: true } : {}),
    ...(output.replayed ? { output_replayed: true } : {}),
    ...(output.originalTokenCount === null ? {} : { original_token_count: output.originalTokenCount }),
    // This adapter emits structuredContent beside the text result, so both representations
    // must obey the same policy/default budget. Returning the retained raw buffer here made
    // the schema path bypass the model-visible truncation entirely.
    output: truncatedOutput(output, modelOutputMaxTokens(output))
  };
}

// --------------------------------------------------------------------------- manager

export interface ExecCommandRequest {
  classifyExit?: (exitCode: number | null, rawOutput: string) => boolean;
  batchMarker?: string;
  command: string[];
  shellType: ShellType;
  hookCommand: string;
  processId: number;
  yieldTimeMs: number;
  maxOutputTokens: number | undefined;
  truncationPolicy: TruncationPolicy;
  cwd: string;
  /** The virtual path this ran in, kept only so `session status` can name it. */
  displayCwd: string;
  env: NodeJS.ProcessEnv;
  tty: boolean;
}

export interface WriteStdinRequest {
  processId: number;
  input: string;
  yieldTimeMs: number;
  maxOutputTokens: number | undefined;
  truncationPolicy: TruncationPolicy;
  maxWriteStdinYieldTimeMs?: number;
}

export interface BackgroundTerminalInfo {
  processId: number;
  incarnation: number;
  command: string;
  cwd: string;
  pid: number;
  tty: boolean;
  startedAt: number;
}

interface ProcessEntry {
  classifyExit?: ExecCommandRequest['classifyExit'];
  batchMarker?: string;
  process: UnifiedExecProcess;
  processId: number;
  incarnation: number;
  cwd: string;
  hookCommand: string;
  tty: boolean;
  startedAt: number;
  initialExecCommandActive: boolean;
  termination?: Promise<void>;
}

/** Local response completion, not a claim that a remote model understood the output. */
export interface OutputPublication {
  completedAt: number | null;
  failed: boolean;
}

export interface CompletedOutputPage {
  processId: number;
  command: string;
  exitCode: number | null;
  output: string;
  start: number;
  end: number;
  total: number;
}

export interface BackgroundExecState {
  running: number[];
  exitedUnread: Array<{ processId: number; exitCode: number | null }>;
}

type OutputSnapshot = Pick<ExecCommandToolOutput, 'rawOutput' | 'displayOutput' | 'originalTokenCount' | 'outputOmittedBytes'>;

interface CompletedResult {
  identity: object;
  expiresAt: number;
  command: string;
  exitCode: number | null;
  benignExit: boolean;
  replay: Pick<OutputSnapshot, 'rawOutput' | 'displayOutput'>;
  unread?: OutputSnapshot & {
    offset: number;
    offer?: { end: number; publication: OutputPublication };
  };
}

function snapshotOutput(collected: ReturnType<UnifiedExecProcess['takeBuffer']>): OutputSnapshot {
  const visible = collected.display ?? collected.raw;
  return {
    rawOutput: collected.raw.toBytesWithOmissionMarker(),
    ...(collected.display ? { displayOutput: collected.display.toBytesWithOmissionMarker() } : {}),
    originalTokenCount: approxTokensFromByteCount(visible.totalBytes()),
    outputOmittedBytes: visible.omittedBytes() || null
  };
}

function boundCompletedBuffer(buffer: HeadTailBuffer): HeadTailBuffer {
  const bounded = new HeadTailBuffer(COMPLETED_EXEC_OUTPUT_BYTES);
  bounded.pushBuffer(buffer);
  return bounded;
}

export class UnifiedExecProcessManager {
  private readonly processes = new Map<number, ProcessEntry>();
  private readonly completed = new Map<number, CompletedResult>();
  private completedExpiry?: NodeJS.Timeout;
  private readonly pendingSpawns = new Map<number, Promise<UnifiedExecProcess>>();
  private shutdown?: Promise<void>;
  private releaseListener?: (processId: number) => void;
  private processChangeListener?: () => void;
  private readonly reservedProcessIds = new Set<number>();
  private nextProcessIncarnation = 1;
  private readonly maxWriteStdinYieldTimeMs: number;

  constructor(maxWriteStdinYieldTimeMs: number) {
    this.maxWriteStdinYieldTimeMs = Math.max(maxWriteStdinYieldTimeMs, MIN_EMPTY_YIELD_TIME_MS);
  }

  /** The custody registry drops ownership only when this manager really discards an id. */
  setProcessReleaseListener(listener: (processId: number) => void): void {
    this.releaseListener = listener;
  }

  /** Presentation observers may reread the manager after a process starts, exits or is discarded. */
  setProcessChangeListener(listener: () => void): void {
    this.processChangeListener = listener;
  }

  private notifyProcessChange(): void {
    this.processChangeListener?.();
  }

  /**
   * Reserve a numeric id just before launch. The id names a session and grants no authority. A
   * retained result keeps its id reserved independently of active process capacity.
   */
  allocateProcessId(draw: () => number = () => randomInt(1_000, 100_000)): number {
    for (;;) {
      const processId = draw();
      if (this.reservedProcessIds.has(processId) || this.completed.has(processId)) continue;
      this.reservedProcessIds.add(processId);
      return processId;
    }
  }

  releaseProcessId(processId: number): void {
    const reserved = this.reservedProcessIds.delete(processId);
    this.processes.delete(processId);
    const retained = this.completed.delete(processId);
    if (!reserved && !retained) return;
    this.releaseListener?.(processId);
    this.notifyProcessChange();
  }

  processIdentity(processId: number): object | undefined {
    return this.processes.get(processId)?.process.identity ?? this.completed.get(processId)?.identity;
  }

  private scheduleCompletedExpiry(): void {
    if (this.completedExpiry) return;
    const oldest = this.completed.values().next().value;
    if (!oldest) return;
    this.completedExpiry = setTimeout(() => {
      this.completedExpiry = undefined;
      const now = performance.now();
      for (const [id, result] of this.completed) {
        if (result.expiresAt > now) break;
        this.releaseProcessId(id);
      }
      this.scheduleCompletedExpiry();
    }, Math.max(1, Math.ceil(oldest.expiresAt - performance.now())));
    this.completedExpiry.unref();
  }

  private retainCompleted(entry: ProcessEntry, unread: boolean): void {
    if (this.processes.get(entry.processId) !== entry) return;
    const { process } = entry;
    const rawOutput = process.retainedOutput();
    const benignExit = process.benignExit(entry.classifyExit);
    const exitCode = process.exitCode();
    let pending: OutputSnapshot | undefined;
    if (unread) {
      const drained = process.takeBuffer();
      pending = snapshotOutput({
        raw: boundCompletedBuffer(drained.raw),
        display: drained.display && boundCompletedBuffer(drained.display)
      });
    }
    process.discardCapturedOutput();

    this.processes.delete(entry.processId);
    this.reservedProcessIds.delete(entry.processId);
    this.completed.set(entry.processId, {
      identity: process.identity,
      command: entry.hookCommand,
      expiresAt: performance.now() + COMPLETED_EXEC_RETENTION_MS,
      replay: {
        rawOutput,
        ...(entry.batchMarker ? { displayOutput: new CommandBatchDisplay(entry.batchMarker).push(rawOutput, true) } : {})
      },
      exitCode,
      benignExit,
      ...(pending ? { unread: { ...pending, offset: 0 } } : {})
    });
    while (this.completed.size > MAX_COMPLETED_EXEC_RESULTS) this.releaseProcessId(this.completed.keys().next().value!);
    this.scheduleCompletedExpiry();
    this.notifyProcessChange();
  }

  /** Move an exited, settled session to the output cache without consuming its unread bytes. */
  private retireIfIdle(entry: ProcessEntry): void {
    if (entry.initialExecCommandActive || entry.termination ||
        !entry.process.hasExited() || !entry.process.outputClosed ||
        this.processes.get(entry.processId) !== entry) return;
    const release = entry.process.interactionLock.tryLock();
    if (!release) return;
    try { this.retainCompleted(entry, true); } finally { release(); }
  }

  async execCommand(request: ExecCommandRequest): Promise<ExecCommandToolOutput> {
    const { processId } = request;
    if (!this.reservedProcessIds.has(processId) || this.pendingSpawns.has(processId) || this.processes.has(processId)) {
      throw UnifiedExecError.createProcess('terminal session reservation was lost before launch');
    }
    let spawning: Promise<UnifiedExecProcess> | undefined;
    let process: UnifiedExecProcess;
    try {
      this.ensureProcessCapacity();
      // `UnifiedExecRuntime::run` prefixes every PowerShell script before it reaches the
      // process launcher so pipe-mode output is UTF-8 just like PTY output.
      // The app also ends it with the exit status of its last statement (see withPowershellExitStatus).
      const command =
        request.shellType === 'powershell' ? withPowershellExitStatus(prefixPowershellScriptWithUtf8(request.command)) : request.command;
      spawning = UnifiedExecProcess.spawn({
        batchMarker: request.batchMarker,
        command,
        shellType: request.shellType,
        cwd: request.cwd,
        env: request.env,
        tty: request.tty
      });
      this.pendingSpawns.set(processId, spawning);
      process = await spawning;
    } catch (error) {
      if (!spawning || this.pendingSpawns.get(processId) === spawning) {
        this.pendingSpawns.delete(processId);
        this.releaseProcessId(processId);
      }
      throw error instanceof UnifiedExecError
        ? error
        : UnifiedExecError.createProcess(error instanceof Error ? error.message : String(error));
    }

    if (this.pendingSpawns.get(processId) !== spawning) {
      throw UnifiedExecError.createProcess('terminal manager shut down during launch');
    }
    this.pendingSpawns.delete(processId);

    const start = Date.now();
    const wallStart = performance.now();
    const entry: ProcessEntry = {
      process,
      processId,
      incarnation: this.nextProcessIncarnation++,
      cwd: request.displayCwd,
      hookCommand: request.hookCommand,
      tty: request.tty,
      startedAt: start,
      initialExecCommandActive: true,
      classifyExit: request.classifyExit,
      batchMarker: request.batchMarker
    };
    // Keep custody even when the transport drops the initial call.
    this.processes.set(processId, entry);
    void process.outputSettled.then(() => {
      this.retireIfIdle(entry);
      // Even a concurrent reader may still own the terminal buffer after exit.
      if (this.processes.get(processId) === entry) this.notifyProcessChange();
    });

    try {
      const collected = await collectOutputUntilDeadline(process, start + clampYieldTime(request.yieldTimeMs));
      if (this.processes.get(processId) !== entry) throw UnifiedExecError.unknownProcessId(processId);
      const failure = process.failureMessage();
      if (failure !== null) {
        this.releaseProcessId(processId);
        throw UnifiedExecError.processFailed(failure);
      }
      return await this.processResponse(entry, collected, request, wallStart, true);
    } finally {
      entry.initialExecCommandActive = false;
      this.retireIfIdle(entry);
    }
  }

  private async processResponse(
    entry: ProcessEntry,
    collected: ReturnType<UnifiedExecProcess['takeBuffer']>,
    request: Pick<ExecCommandRequest, 'truncationPolicy' | 'maxOutputTokens'>,
    wallStart: number,
    initial = false
  ): Promise<ExecCommandToolOutput> {
    const { process, processId, classifyExit } = entry;
    const exited = process.hasExited();
    if (exited) {
      await process.outputSettled;
      const final = process.takeBuffer();
      collected.raw.pushBuffer(final.raw);
      if (final.display) {
        collected.display ??= new HeadTailBuffer();
        collected.display.pushBuffer(final.display);
      }
    }
    const benignExit = process.benignExit(classifyExit);
    if (exited && (initial || !entry.initialExecCommandActive)) this.retainCompleted(entry, false);
    return {
      ...(exited ? { completedSessionId: processId } : initial ? {
        completion: process.completion.then(async completion => {
          await process.outputSettled;
          return { ...completion, benignExit: process.benignExit(classifyExit) };
        })
      } : {}),
      identity: process.identity,
      benignExit,
      chunkId: generateChunkId(),
      wallTimeMs: Math.max(0, performance.now() - wallStart),
      ...snapshotOutput(collected),
      truncationPolicy: request.truncationPolicy,
      maxOutputTokens: request.maxOutputTokens,
      processId: exited ? null : processId,
      exitCode: process.exitCode()
    };
  }

  async writeStdin(request: WriteStdinRequest): Promise<ExecCommandToolOutput> {
    const replay = (identity?: object): ExecCommandToolOutput | null => {
      const saved = this.completed.get(request.processId);
      if (!saved) return null;
      if (identity && saved.identity !== identity) throw UnifiedExecError.unknownProcessId(request.processId);
      if (request.input !== '') throw UnifiedExecError.processFailed('Process already completed; no input was sent. Use empty chars to read its retained output.');
      const { unread } = saved;
      const { rawOutput, displayOutput } = unread ?? saved.replay;
      const offset = unread?.offset ?? 0;
      saved.unread = undefined;
      return {
        rawOutput: offset > 0 ? (displayOutput ?? rawOutput).subarray(offset) : rawOutput,
        ...(displayOutput ? { displayOutput: displayOutput.subarray(offset) } : {}),
        benignExit: saved.benignExit,
        exitCode: saved.exitCode,
        chunkId: generateChunkId(), wallTimeMs: 0, processId: null, completedSessionId: request.processId,
        ...(unread ? {} : { replayed: true }),
        originalTokenCount: unread?.originalTokenCount ?? approxTokenCount((displayOutput ?? rawOutput).toString('utf8')),
        outputOmittedBytes: unread?.outputOmittedBytes ?? null,
        truncationPolicy: request.truncationPolicy, maxOutputTokens: request.maxOutputTokens
      };
    };
    const saved = replay();
    if (saved) return saved;
    const entry = this.processes.get(request.processId);
    if (!entry) throw UnifiedExecError.unknownProcessId(request.processId);
    const { process, tty } = entry;

    // Polls share the live draining buffer. If the first poll retires it, later
    // queued callers may reread that exact result and never a recycled id.
    const release = await process.interactionLock.lock();
    try {
      const saved = replay(process.identity);
      if (saved) return saved;
      if (this.processes.get(request.processId) !== entry) throw UnifiedExecError.unknownProcessId(request.processId);

      if (request.input !== '') {
        if (process.hasExited()) throw UnifiedExecError.processFailed('Process already completed; no input was sent. Use empty chars to read its retained output.');
        if (!tty) {
          if (request.input !== INTERRUPT) throw UnifiedExecError.stdinClosed();
          await process.interrupt();
        } else {
          try {
            await process.write(request.input);
            // A brief window so the child's reaction is more likely to land in the poll below.
            await sleep(100);
          } catch (error) {
            if (!process.hasExited()) throw error;
          }
        }
      }

      const maxEmptyYield = Math.max(
        request.maxWriteStdinYieldTimeMs ?? this.maxWriteStdinYieldTimeMs,
        MIN_EMPTY_YIELD_TIME_MS
      );
      const base = Math.max(request.yieldTimeMs, MIN_YIELD_TIME_MS);
      const yieldTimeMs =
        request.input === ''
          ? Math.min(Math.max(base, MIN_EMPTY_YIELD_TIME_MS), maxEmptyYield)
          : Math.min(base, MAX_YIELD_TIME_MS);

      const start = Date.now();
      const wallStart = performance.now();
      // Empty calls return as soon as live output arrives; non-empty writes keep
      // the collection window so the response to an interactive input is gathered.
      const collected = await collectOutputUntilDeadline(process, start + yieldTimeMs, request.input === '');
      const failure = process.failureMessage();
      if (failure !== null) {
        if (this.processes.get(request.processId) === entry) this.releaseProcessId(request.processId);
        throw UnifiedExecError.processFailed(failure);
      }
      if (!process.hasExited() && this.processes.get(request.processId) !== entry) {
        throw UnifiedExecError.unknownProcessId(request.processId);
      }
      return await this.processResponse(entry, collected, request, wallStart);
    } finally {
      release();
      this.retireIfIdle(entry);
    }
  }

  /** Live sessions, oldest id first. */
  /**
   * A live, read-only view of a running command's output for the person (#1920): the output so far,
   * then each new chunk, then its exit. Nothing is consumed, so the agent's own polls read exactly
   * what they would have. Null when that exact process (id and incarnation) is not running.
   */
  watchOutput(processId: number, incarnation: number, onChunk: (chunk: Buffer) => void,
    onExit: (completion: ProcessCompletion) => void): { history: Buffer; startedAt: number; unsubscribe: () => void } | null {
    const entry = this.processes.get(processId);
    if (!entry || entry.incarnation !== incarnation) return null;
    let watching = true;
    let lastChunkAt = 0;
    const process = entry.process;
    const stop = process.watch(chunk => { lastChunkAt = Date.now(); onChunk(chunk); });
    void process.completion.then(async completion => {
      // A pipe's streams can still be draining when it exits: the same bounded wait as a completed read.
      if (!process.outputClosed) {
        const closed = process.outputClosedNotify.notified();
        try { await raceWithTimeout([closed], POST_EXIT_CLOSE_WAIT_CAP_MS); } finally { closed.dispose(); }
      }
      if (process.tty) {
        const exitedAt = Date.now();
        while (watching && Date.now() - exitedAt < WATCH_EXIT_QUIET_CAP_MS) {
          const quietFor = Date.now() - Math.max(lastChunkAt, exitedAt);
          if (quietFor >= WATCH_EXIT_QUIET_MS) break;
          await new Promise(resolve => setTimeout(resolve, WATCH_EXIT_QUIET_MS - quietFor));
        }
      }
      try { if (watching) onExit(completion); } finally { stop(); }
    });
    return { history: entry.process.historyBytes(), startedAt: entry.startedAt, unsubscribe: () => { watching = false; stop(); } };
  }

  listProcesses(): BackgroundTerminalInfo[] {
    return [...this.processes.values()]
      .filter((entry) => !entry.process.hasExited())
      .sort((left, right) => left.processId - right.processId)
      .map((entry) => ({
        processId: entry.processId,
        incarnation: entry.incarnation,
        command: entry.hookCommand,
        cwd: entry.cwd,
        pid: entry.process.pid,
        tty: entry.tty,
        startedAt: entry.startedAt
      }));
  }

  /** Non-destructive obligation view for a caller-owned set of retained sessions. */
  backgroundState(processIds: ReadonlySet<number>): BackgroundExecState {
    const running: number[] = [];
    const exitedUnread: Array<{ processId: number; exitCode: number | null }> = [];
    for (const id of processIds) {
      const entry = this.processes.get(id);
      if (entry && !entry.initialExecCommandActive && !entry.process.hasExited()) running.push(id);
      const result = this.completed.get(id);
      if (result?.unread) exitedUnread.push({ processId: id, exitCode: result.exitCode });
    }
    return {
      running: running.sort((left, right) => left - right),
      exitedUnread: exitedUnread.sort((left, right) => left.processId - right.processId)
    };
  }

  /** Exited rows whose saved unread output has not been delivered. */
  exitedUnread(processIds: ReadonlySet<number>): Array<{ processId: number; exitCode: number | null }> {
    return this.backgroundState(processIds).exitedUnread;
  }

  /** A later owner call acknowledges a successfully published page, even within one request ID. */
  async acknowledgeCompletedOutput(processIds: ReadonlySet<number>, startedAt: number, except?: number): Promise<number[]> {
    const delivered: number[] = [];
    for (const id of processIds) {
      if (id === except) continue; // Explicit polling still owns its normal result path.
      const result = this.completed.get(id);
      const unread = result?.unread;
      const offered = unread?.offer;
      if (!result || !unread || !offered || offered.publication.failed || offered.publication.completedAt === null ||
          startedAt <= offered.publication.completedAt) continue;
      unread.offset = offered.end;
      unread.offer = undefined;
      if (offered.end >= (unread.displayOutput ?? unread.rawOutput).length) {
        result.unread = undefined;
        delivered.push(id);
      }
    }
    return delivered;
  }

  /** Offer one bounded page. A failed response leaves its bytes available for the next attempt. */
  async offerCompletedOutput(
    processIds: ReadonlySet<number>, publication: OutputPublication, maxBytes: number
  ): Promise<CompletedOutputPage | null> {
    if (publication.failed || maxBytes < 4) return null;
    for (const id of [...processIds].sort((left, right) => left - right)) {
      const result = this.completed.get(id);
      const unread = result?.unread;
      if (!result || !unread || (unread.offer && !unread.offer.publication.failed)) continue;
      const output = unread.displayOutput ?? unread.rawOutput;
      const start = unread.offset;
      let end = Math.min(output.length, start + Math.floor(maxBytes));
      // Do not split a UTF-8 character across separately rendered MCP responses.
      while (end > start && end < output.length && (output[end]! & 0xc0) === 0x80) end--;
      if (end === start && output.length > start) continue;
      unread.offer = { end, publication };
      return { processId: id, command: result.command, exitCode: result.exitCode,
        output: output.subarray(start, end).toString('utf8'), start, end, total: output.length };
    }
    return null;
  }

  async terminateProcess(processId: number, expectedIncarnation?: number): Promise<boolean> {
    const entry = this.processes.get(processId);
    if (!entry || entry.process.hasExited() ||
        (expectedIncarnation !== undefined && entry.incarnation !== expectedIncarnation)) return false;

    entry.termination ??= entry.process.terminate();
    try {
      await entry.termination;
      // The initial exec still owns its response and will collect the exit.
      if (this.processes.get(processId) === entry && !entry.initialExecCommandActive) this.releaseProcessId(processId);
      return true;
    } finally {
      entry.termination = undefined;
      this.retireIfIdle(entry);
    }
  }

  terminateAllProcesses(): Promise<void> {
    if (this.shutdown) return this.shutdown;
    const pending = [...this.pendingSpawns.values()];
    const entries = [...this.processes.values()];
    const ids = new Set([...this.reservedProcessIds, ...this.completed.keys()]);
    this.pendingSpawns.clear();
    this.completed.clear();
    this.processes.clear();
    this.reservedProcessIds.clear();
    clearTimeout(this.completedExpiry);
    this.completedExpiry = undefined;
    // Shutdown owns pending spawns as well as registered processes. The launch continuation
    // checks its reservation before publishing any process that completed startup after this.
    this.shutdown = Promise.allSettled([
      ...pending.map(spawned => spawned.then(child => child.hasExited() ? undefined : child.terminate())),
      ...entries.map(entry => entry.termination ?? (entry.process.hasExited() ? undefined : entry.process.terminate()))
    ]).then(() => { this.shutdown = undefined; });
    for (const id of ids) this.releaseListener?.(id);
    if (ids.size) this.notifyProcessChange();
    return this.shutdown;
  }

  private ensureProcessCapacity(): void {
    if (this.shutdown) throw UnifiedExecError.createProcess('terminal manager is shutting down');
    // Count the reservation before the async spawn starts. An observed exit releases capacity
    // immediately, even if output is settling or a reader is still collecting its final bytes.
    let active = this.reservedProcessIds.size;
    for (const entry of this.processes.values()) {
      if (!entry.process.hasExited()) continue;
      active--;
      this.retireIfIdle(entry);
    }
    if (active > MAX_UNIFIED_EXEC_PROCESSES) {
      throw UnifiedExecError.createProcess(
        `active terminal session limit reached (${MAX_UNIFIED_EXEC_PROCESSES} starting or running); wait for a command to finish or stop one of your own sessions, then retry`
      );
    }
  }
}

/**
 * `collect_output_until_deadline`, port for port.
 *
 * The post-exit grace is the subtle part: once the process has signalled exit, the loop stops
 * waiting the full deadline and gives the output stream at most 50 ms more to close, so a
 * command that finished in 20 ms does not spend the whole 10 s yield window proving it.
 */
async function collectOutputUntilDeadline(
  process: UnifiedExecProcess,
  deadline: number,
  returnOnFirstOutput = false
): Promise<{ raw: HeadTailBuffer; display?: HeadTailBuffer }> {
  const collected = new HeadTailBuffer();
  let display: HeadTailBuffer | undefined;
  let exitSignalReceived = process.cancelled;
  let postExitDeadline: number | null = null;

  for (;;) {
    // Drained and re-armed in one synchronous step, so a chunk arriving between the two
    // cannot be missed by the wait that follows.
    const drained = process.takeBuffer();
    if (drained.display) {
      display ??= new HeadTailBuffer();
      display.pushBuffer(drained.display);
    }
    const hasDrainedOutput = drained.raw.retainedBytes() > 0 || drained.raw.omittedBytes() > 0;
    const waitForOutput = hasDrainedOutput ? null : process.outputNotify.notified();

    if (!hasDrainedOutput) {
      exitSignalReceived ||= process.cancelled;
      if (exitSignalReceived && process.outputClosed) {
        waitForOutput?.dispose();
        break;
      }
      const now = Date.now();
      const remaining = Math.max(0, deadline - now);
      if (remaining === 0) {
        waitForOutput?.dispose();
        break;
      }

      if (exitSignalReceived) {
        postExitDeadline ??= now + Math.min(remaining, POST_EXIT_CLOSE_WAIT_CAP_MS);
        const closeWaitRemaining = Math.max(0, postExitDeadline - now);
        if (closeWaitRemaining === 0) {
          waitForOutput?.dispose();
          break;
        }
        const closed = process.outputClosedNotify.notified();
        const timedOut = await raceWithTimeout([waitForOutput, closed], closeWaitRemaining);
        waitForOutput?.dispose();
        closed.dispose();
        if (timedOut) break;
        continue;
      }

      const exitNotified = process.cancelNotify.notified();
      const timedOut = await raceWithTimeout([waitForOutput, exitNotified], remaining);
      waitForOutput?.dispose();
      exitNotified.dispose();
      if (timedOut) break;
      exitSignalReceived ||= process.cancelled;
      continue;
    }

    collected.pushBuffer(drained.raw);
    if (returnOnFirstOutput) break;
    exitSignalReceived ||= process.cancelled;
    if (Date.now() >= deadline) break;
  }

  return { raw: collected, display };
}

/** Resolves true when the timeout won the race. */
async function raceWithTimeout(
  waits: ReadonlyArray<{ promise: Promise<void> } | null>,
  timeoutMs: number
): Promise<boolean> {
  let timer: NodeJS.Timeout | null = null;
  const timeout = new Promise<boolean>((resolve) => {
    timer = setTimeout(() => resolve(true), Math.max(0, timeoutMs));
    if (typeof timer.unref === 'function') timer.unref();
  });
  try {
    const races = waits.filter((wait): wait is { promise: Promise<void> } => wait !== null).map((wait) =>
      wait.promise.then(() => false)
    );
    return await Promise.race([...races, timeout]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
