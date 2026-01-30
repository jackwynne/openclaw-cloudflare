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

  // Mount R2 if not already mounted
  const mounted = await mountR2Storage(sandbox, env);
  if (!mounted) {
    return { success: false, error: 'Failed to mount R2 storage' };
  }

  // Sanity check: verify source has critical files before syncing
  // This prevents accidentally overwriting a good backup with empty/corrupted data
  try {
    const checkProc = await sandbox.startProcess('test -f /root/.openclaw/openclaw.json && echo "ok"');
    await waitForProcess(checkProc, 5000);
    const checkLogs = await checkProc.getLogs();
    if (!checkLogs.stdout?.includes('ok')) {
      return { 
        success: false, 
        error: 'Sync aborted: source missing openclaw.json',
        details: 'The local config directory is missing critical files. This could indicate corruption or an incomplete setup.',
      };
    }
  } catch (err) {
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
    const proc = await sandbox.startProcess(syncCmd);
    await waitForProcess(proc, 120000); // 2 minute timeout for sync (cron + large backups can take longer)

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
    
    if (lastSync && lastSync.match(/^\d{4}-\d{2}-\d{2}/)) {
      return { success: true, lastSync };
    } else {
      const logs = await proc.getLogs();
      const diagProc = await sandbox.startProcess(
        `echo "[diag] mount:" && mount | grep s3fs || true; ` +
        `echo "[diag] r2_path:" && ls -la ${R2_MOUNT_PATH} || true; ` +
        `echo "[diag] openclaw_src:" && ls -la /root/.openclaw || true; ` +
        `echo "[diag] skills_src:" && ls -la /root/openclaw/skills || true`
      );
      await waitForProcess(diagProc, 5000);
      const diagLogs = await diagProc.getLogs();
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
    return { 
      success: false, 
      error: 'Sync error',
      details: err instanceof Error ? err.message : 'Unknown error',
    };
  }
}
