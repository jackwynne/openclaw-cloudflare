import type { Sandbox } from '@cloudflare/sandbox';
import type { OpenClawEnv } from '../types';
import { R2_MOUNT_PATH } from '../config';
import { mountR2Storage } from './r2';
import { waitForProcess } from './utils';

export interface SyncResult {
  success: boolean;
  lastSync?: string;
  error?: string;
  details?: string;
}

function isDebugEnabled(env: OpenClawEnv): boolean {
  return env.DEBUG_ROUTES === 'true' || env.DEV_MODE === 'true';
}

function debugLog(env: OpenClawEnv, event: string, data?: Record<string, unknown>): void {
  if (!isDebugEnabled(env)) return;
  // Log as single-line JSON for Cloudflare log aggregation.
  console.log(JSON.stringify({ msg: '[syncToR2]', event, ...data }));
}

function truncate(s: string, max: number): string {
  if (s.length <= max) return s;
  return s.slice(0, max) + `…(+${s.length - max} chars)`;
}

/**
 * Sync openclaw config from container to R2 for persistence.
 * 
 * This function:
 * 1. Mounts R2 if not already mounted
 * 2. Verifies source has critical files (prevents overwriting good backup with empty data)
 * 3. Runs rsync to copy config to R2
 * 4. Writes a timestamp file for tracking
 * 
 * @param sandbox - The sandbox instance
 * @param env - Worker environment bindings
 * @returns SyncResult with success status and optional error details
 */
export async function syncToR2(sandbox: Sandbox, env: OpenClawEnv): Promise<SyncResult> {
  // Check if R2 is configured
  if (!env.R2_ACCESS_KEY_ID || !env.R2_SECRET_ACCESS_KEY || !env.CF_ACCOUNT_ID) {
    return { success: false, error: 'R2 storage is not configured' };
  }

  debugLog(env, 'start', {
    r2MountPath: R2_MOUNT_PATH,
    hasR2AccessKeyId: !!env.R2_ACCESS_KEY_ID,
    hasR2SecretAccessKey: !!env.R2_SECRET_ACCESS_KEY,
    hasCfAccountId: !!env.CF_ACCOUNT_ID,
  });

  // Mount R2 if not already mounted
  const mounted = await mountR2Storage(sandbox, env);
  if (!mounted) {
    debugLog(env, 'mount_failed');
    return { success: false, error: 'Failed to mount R2 storage' };
  }
  debugLog(env, 'mount_ok');

  // Sanity check: verify source has critical files before syncing
  // This prevents accidentally overwriting a good backup with empty/corrupted data
  try {
    // Important: sandbox process status can lag behind actual completion.
    // For tiny commands like `test -f`, rely on log output instead of status.
    const verifyCmd =
      `sh -lc 'if test -f /root/.openclaw/openclaw.json; then echo "__OK__"; else echo "__MISSING__"; fi'`;
    const checkProc = await sandbox.startProcess(verifyCmd);
    debugLog(env, 'verify_started', { cmd: verifyCmd, status: checkProc.status });

    // Best-effort wait: ignore timeouts and poll logs.
    await waitForProcess(checkProc, 10_000, 200).catch(() => {});

    let stdout = '';
    let stderr = '';
    for (let i = 0; i < 50; i++) {
      const logs = await checkProc.getLogs().catch(() => ({ stdout: '', stderr: '' }));
      stdout = logs.stdout || '';
      stderr = logs.stderr || '';
      if (stdout.includes('__OK__') || stdout.includes('__MISSING__')) break;
      await new Promise((r) => setTimeout(r, 100));
    }

    debugLog(env, 'verify_result', {
      status: checkProc.status,
      exitCode: (checkProc as unknown as { exitCode?: number }).exitCode,
      stdout: truncate(stdout.trim(), 500),
      stderr: truncate(stderr.trim(), 500),
    });

    if (stdout.includes('__MISSING__')) {
      return { 
        success: false, 
        error: 'Sync aborted: source missing openclaw.json',
        details: 'The local config directory is missing critical files. This could indicate corruption or an incomplete setup.',
      };
    }

    if (!stdout.includes('__OK__')) {
      throw new Error(
        `Timed out waiting for source verification output (status=${checkProc.status}). stdout=${JSON.stringify(stdout.trim())} stderr=${JSON.stringify(stderr.trim())}`
      );
    }
  } catch (err) {
    debugLog(env, 'verify_error', { error: err instanceof Error ? err.message : String(err) });
    return { 
      success: false, 
      error: 'Failed to verify source files',
      details: err instanceof Error ? err.message : 'Unknown error',
    };
  }

  // Run rsync to backup config to R2
  // Note: Use --no-times because s3fs doesn't support setting timestamps
  const syncCmd = `rsync -r --no-times --delete --exclude='*.lock' --exclude='*.log' --exclude='*.tmp' /root/.openclaw/ ${R2_MOUNT_PATH}/openclaw/ && rsync -r --no-times --delete /root/openclaw/skills/ ${R2_MOUNT_PATH}/skills/ && date -Iseconds > ${R2_MOUNT_PATH}/.last-sync`;
  
  try {
    debugLog(env, 'rsync_started', { cmd: syncCmd });
    const proc = await sandbox.startProcess(syncCmd);
    debugLog(env, 'rsync_process', { status: proc.status });

    try {
      await waitForProcess(proc, 120000); // 2 minute timeout for sync (cron + large backups can take longer)
    } catch (e) {
      // Include logs/status to help diagnose timeouts or stuck rsync.
      const logs = await proc.getLogs().catch(() => ({ stdout: '', stderr: '' }));
      debugLog(env, 'rsync_wait_error', {
        error: e instanceof Error ? e.message : String(e),
        status: proc.status,
        exitCode: (proc as unknown as { exitCode?: number }).exitCode,
        stdout: truncate((logs.stdout || '').trim(), 2000),
        stderr: truncate((logs.stderr || '').trim(), 2000),
      });
      throw e;
    }
    debugLog(env, 'rsync_wait_done', {
      status: proc.status,
      exitCode: (proc as unknown as { exitCode?: number }).exitCode,
    });

    // Check for success by reading the timestamp file.
    // Do not rely on process status alone: status/logs can lag behind actual completion.
    // Note: backup structure is ${R2_MOUNT_PATH}/openclaw/ and ${R2_MOUNT_PATH}/skills/
    const timestampProc = await sandbox.startProcess(
      // Single process to avoid status/log race conditions and avoid spinning up many processes.
      // Wait up to ~10 seconds for the marker to appear.
      `sh -lc 'for i in $(seq 1 40); do if test -f "${R2_MOUNT_PATH}/.last-sync"; then cat "${R2_MOUNT_PATH}/.last-sync"; exit 0; fi; sleep 0.25; done; echo "__MISSING__"'`
    );
    await waitForProcess(timestampProc, 15000);
    const timestampLogs = await timestampProc.getLogs();
    const out = timestampLogs.stdout?.trim();
    const lastSync = out && out !== '__MISSING__' ? out : undefined;

    debugLog(env, 'timestamp_result', {
      status: timestampProc.status,
      exitCode: (timestampProc as unknown as { exitCode?: number }).exitCode,
      stdout: truncate((timestampLogs.stdout || '').trim(), 500),
      stderr: truncate((timestampLogs.stderr || '').trim(), 500),
    });
    
    if (lastSync && lastSync.match(/^\d{4}-\d{2}-\d{2}/)) {
      return { success: true, lastSync };
    } else {
      const logs = await proc.getLogs();
      const diagProc = await sandbox.startProcess(
        // Run under a shell because this command uses `&&`, `||`, and pipes.
        `sh -lc ` +
        `"echo '[diag] mount:' && mount | grep s3fs || true; ` +
        `echo '[diag] r2_path:' && ls -la ${R2_MOUNT_PATH} || true; ` +
        `echo '[diag] openclaw_src:' && ls -la /root/.openclaw || true; ` +
        `echo '[diag] skills_src:' && ls -la /root/openclaw/skills || true"`
      );
      await waitForProcess(diagProc, 5000, 200).catch(() => {});
      const diagLogs = await diagProc.getLogs().catch(() => ({ stdout: '', stderr: '' }));
      debugLog(env, 'timestamp_missing_diag', {
        rsyncStatus: proc.status,
        rsyncExitCode: (proc as unknown as { exitCode?: number }).exitCode,
        rsyncStdout: truncate((logs.stdout || '').trim(), 2000),
        rsyncStderr: truncate((logs.stderr || '').trim(), 2000),
        diagStdout: truncate((diagLogs.stdout || '').trim(), 2000),
        diagStderr: truncate((diagLogs.stderr || '').trim(), 2000),
      });
      return {
        success: false,
        error: 'Sync failed',
        details: [
          logs.stderr || logs.stdout || 'No timestamp file created',
          diagLogs.stdout?.trim(),
          diagLogs.stderr?.trim(),
        ].filter(Boolean).join('\n'),
      };
    }
  } catch (err) {
    debugLog(env, 'sync_error', { error: err instanceof Error ? err.message : String(err) });
    return { 
      success: false, 
      error: 'Sync error',
      details: err instanceof Error ? err.message : 'Unknown error',
    };
  }
}
