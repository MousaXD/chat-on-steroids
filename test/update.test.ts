import { afterEach, expect, it, vi } from 'vitest';
import * as updater from '../src/main/update.js';
import { APP_VERSION } from '../src/main/version.js';

afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers(); });

it('disables startup, periodic and explicit application update checks', async () => {
  vi.useFakeTimers();
  const fetch = vi.fn(() => { throw new Error('unexpected network'); });
  vi.stubGlobal('fetch', fetch);
  const listener = vi.fn();
  updater.onUpdateChange(listener);
  updater.startUpdateChecks();
  await updater.checkForUpdates();
  await updater.checkForUpdatesIfStale();
  await vi.advanceTimersByTimeAsync(24 * 60 * 60 * 1000);
  expect(fetch).not.toHaveBeenCalled();
  expect(vi.getTimerCount()).toBe(0);
  expect(listener).not.toHaveBeenCalled();
  expect(updater.updateStatus()).toEqual({ current: APP_VERSION, latest: null, stage: 'idle', error: null, checkedAt: null, disabled: true });
});

it('refuses manual downloads and installer handoff', async () => {
  const takeover = vi.fn(), step = vi.fn();
  updater.onInstallerTakeover(takeover);
  updater.onUpdateHandoffStep(step);
  expect(() => updater.manualDownloadUrl('99.0.0')).toThrow('disabled');
  expect(updater.markInstallOnQuit('99.0.0')).toBe(false);
  expect(updater.markInstallOnQuit()).toBe(false);
  expect(updater.installingVersion()).toBeNull();
  await updater.applyStagedUpdate();
  expect(takeover).not.toHaveBeenCalled();
  expect(step).not.toHaveBeenCalled();
});
