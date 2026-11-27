// This fork intentionally has no application updater. Keep the existing IPC/lifecycle
// contract inert: no network, timers, cache access, installer handoff or executable swap.
import { APP_VERSION } from './version.js';
import type { UpdateStatus } from '../shared/types.js';

export function updateStatus(): UpdateStatus {
  return { current: APP_VERSION, latest: null, stage: 'idle', error: null, checkedAt: null, disabled: true };
}
export function onUpdateChange(_listener: () => void): () => void { return () => {}; }
export function startUpdateChecks(): void {}
export async function checkForUpdatesIfStale(): Promise<void> {}
export async function checkForUpdates(): Promise<void> {}
export function manualDownloadUrl(_version: string | null): never {
  throw new Error('Application updates are disabled in this build');
}
export function markInstallOnQuit(_expectedVersion?: string): boolean { return false; }
export function installingVersion(): null { return null; }
export type UpdateHandoffStep = 'permission' | 'installer';
export const ELEVATION_ANSWER_MS = 10 * 60_000;
export function onUpdateHandoffStep(_listener: (step: UpdateHandoffStep) => void): void {}
export function onInstallerTakeover(_wait: () => Promise<void>): void {}
export async function applyStagedUpdate(): Promise<void> {}
