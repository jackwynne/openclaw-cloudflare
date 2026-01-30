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

const DEFAULT_SYNC_TIMEOUT_MS = 10 * 60 * 1000; // 10 minutes
const OPENCLAW_CONFIG_DIR = '/root/.openclaw';
const OPENCLAW_WORKSPACE_DIR = '/root/openclaw';

function isDebugEnabled(env: OpenClawEnv): boolean {
  return env.DEBUG_ROUTES === 'true' || env.DEV_MODE === 'true';
}

type SyncLogEvent = {
  t: number; // elapsed ms
  event: string;
  data?: Record<string, unknown>;
};

function safeJsonStringify(x: unknown): string {
  try {
    return JSON.stringify(x);
  } catch {
    return '"[unserializable]"';
  }
}

function sanitizeLogData(data?: Record<string, unknown>): Record<string, unknown> | undefined {
  if (!data) return undefined;
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(data)) {
    if (typeof v === 'string') out[k] = truncate(v, 2000);
    else out[k] = v;
  }
  return out;
}

function createSyncLogger(env: OpenClawEnv): {
  runId: string;
  startedAt: number;
  log: (event: string, data?: Record<string, unknown>) => void;
  flushBundle: (finalResult?: SyncResult) => void;
} {
  const startedAt = Date.now();
  const runId =
    typeof globalThis.crypto?.randomUUID === 'function'
      ? globalThis.crypto.randomUUID()
      : `${Date.now()}-${Math.random().toString(16).slice(2)}`;

  const events: SyncLogEvent[] = [];

  const log = (event: string, data?: Record<string, unknown>): void => {
    const t = Date.now() - startedAt;
    const sanitized = sanitizeLogData({ runId, t, ...data });
    events.push({ t, event, data: sanitizeLogData(data) });
    if (!isDebugEnabled(env)) return;
    console.log(safeJsonStringify({ msg: '[syncToR2]', event, ...sanitized }));
  };

  const flushBundle = (finalResult?: SyncResult): void => {
    if (!isDebugEnabled(env)) return;
    // Emit a single bundled line to make sharing logs easy.
    // Keep it reasonably sized: cap events and truncate deeply nested fields.
    const capped = events.slice(-60);
    console.log(
      safeJsonStringify({
        msg: '[syncToR2]',
        event: 'bundle',
        runId,
        durationMs: Date.now() - startedAt,
        finalResult,
        events: capped,
      })
    );
  };

  return { runId, startedAt, log, flushBundle };
}

function truncate(s: string, max: number): string {
  if (s.length <= max) return s;
  return s.slice(0, max) + `…(+${s.length - max} chars)`;
}

async function bestEffortKill(
  log: (event: string, data?: Record<string, unknown>) => void,
  proc: unknown,
  reason: string
): Promise<void> {
  const maybeKill = (proc as { kill?: unknown }).kill;
  if (typeof maybeKill !== 'function') return;
  try {
    // Important: do NOT use Function.prototype.call/apply here.
    // Sandbox process methods are RPC-backed; calling `.call()` attempts to
    // invoke an RPC method named "call" (which doesn't exist).
    await (proc as { kill: () => Promise<void> }).kill();
    log('process_killed', { reason });
  } catch (err) {
    log('process_kill_error', {
      reason,
      error: err instanceof Error ? err.message : String(err),
    });
  }
}

async function checkForActiveRsync(sandbox: Sandbox, env: OpenClawEnv): Promise<{
  hasActive: boolean;
  details?: string;
}> {
  try {
    const processes = await sandbox.listProcesses();
    const active = processes.find((p) => {
      const status = (p as unknown as { status?: string }).status;
      if (status !== 'starting' && status !== 'running') return false;
      const command = (p as unknown as { command?: string }).command || '';
      return command.includes('rsync') && command.includes(R2_MOUNT_PATH);
    });
    if (!active) return { hasActive: false };

    const id = (active as unknown as { id?: string }).id;
    const command = (active as unknown as { command?: string }).command;
    // Keep using plain console for this one: it’s useful even when debug routes are off.
    // Also: syncToR2 now emits a bundled line for easier sharing when debug is on.
    if (isDebugEnabled(env)) {
      console.log(
        safeJsonStringify({
          msg: '[syncToR2]',
          event: 'rsync_already_running',
          id,
          command: truncate(command || '', 500),
        })
      );
    }
    return {
      hasActive: true,
      details: `Skipped: another rsync appears to be running${id ? ` (id=${id})` : ''}.`,
    };
  } catch (err) {
    // If listProcesses fails, don't block sync; just log and proceed.
    if (isDebugEnabled(env)) {
      console.log(
        safeJsonStringify({
          msg: '[syncToR2]',
          event: 'rsync_check_error',
          error: err instanceof Error ? err.message : String(err),
        })
      );
    }
    return { hasActive: false };
  }
}

function getProcessExitCode(proc: unknown): number | undefined {
  const exitCode = (proc as { exitCode?: unknown }).exitCode;
  return typeof exitCode === 'number' ? exitCode : undefined;
}

function getProcessStatus(proc: unknown): string | undefined {
  const status = (proc as { status?: unknown }).status;
  return typeof status === 'string' ? status : undefined;
}

async function ensureRsyncSuccess(
  log: (event: string, data?: Record<string, unknown>) => void,
  proc: unknown,
  which: 'openclaw' | 'workspace' | 'skills'
): Promise<SyncResult | undefined> {
  const status = getProcessStatus(proc);
  const exitCode = getProcessExitCode(proc);
  if (status === 'failed' || (typeof exitCode === 'number' && exitCode !== 0)) {
    const logs = await (proc as { getLogs?: () => Promise<{ stdout?: string; stderr?: string }> })
      .getLogs?.()
      .catch(() => ({ stdout: '', stderr: '' }));
    log('rsync_failed', {
      which,
      status,
      exitCode,
      stdout: truncate((logs?.stdout || '').trim(), 2000),
      stderr: truncate((logs?.stderr || '').trim(), 2000),
    });
    return {
      success: false,
      error: 'Sync failed',
      details:
        (logs?.stderr || logs?.stdout || `rsync failed (${which}) exitCode=${exitCode ?? 'unknown'}`).trim(),
    };
  }
  return undefined;
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
  const logger = createSyncLogger(env);
  const withBundle = (result: SyncResult): SyncResult => {
    logger.flushBundle(result);
    return result;
  };

  // Check if R2 is configured
  if (!env.R2_ACCESS_KEY_ID || !env.R2_SECRET_ACCESS_KEY || !env.CF_ACCOUNT_ID) {
    return withBundle({ success: false, error: 'R2 storage is not configured' });
  }

  logger.log('start', {
    r2MountPath: R2_MOUNT_PATH,
    hasR2AccessKeyId: !!env.R2_ACCESS_KEY_ID,
    hasR2SecretAccessKey: !!env.R2_SECRET_ACCESS_KEY,
    hasCfAccountId: !!env.CF_ACCOUNT_ID,
  });

  // Mount R2 if not already mounted
  const mounted = await mountR2Storage(sandbox, env);
  if (!mounted) {
    logger.log('mount_failed');
    return withBundle({ success: false, error: 'Failed to mount R2 storage' });
  }
  logger.log('mount_ok');

  // If an earlier cron run timed out, rsync may still be running in the container.
  // Avoid piling up multiple rsync processes.
  const activeRsync = await checkForActiveRsync(sandbox, env);
  if (activeRsync.hasActive) {
    return withBundle({ success: true, details: activeRsync.details });
  }

  // Sanity check: verify source has critical files before syncing
  // This prevents accidentally overwriting a good backup with empty/corrupted data
  try {
    // Important: sandbox process status can lag behind actual completion.
    // For tiny commands like `test -f`, rely on log output instead of status.
    const verifyCmd =
      `sh -lc '` +
      // 1) config file check (existing behavior)
      `if test -f "${OPENCLAW_CONFIG_DIR}/openclaw.json"; then echo "__OK__"; else echo "__MISSING__"; fi; ` +
      // 2) workspace safety: avoid syncing an empty local workspace over a non-empty backup
      `if test -d "${OPENCLAW_WORKSPACE_DIR}" && test -n "$(ls -A "${OPENCLAW_WORKSPACE_DIR}" 2>/dev/null)"; then echo "__WORKSPACE_LOCAL_NONEMPTY__"; else echo "__WORKSPACE_LOCAL_EMPTY__"; fi; ` +
      `if test -d "${R2_MOUNT_PATH}/workspace" && test -n "$(ls -A "${R2_MOUNT_PATH}/workspace" 2>/dev/null)"; then echo "__WORKSPACE_REMOTE_NONEMPTY__"; else echo "__WORKSPACE_REMOTE_EMPTY__"; fi` +
      `'`;
    const checkProc = await sandbox.startProcess(verifyCmd);
    logger.log('verify_started', { cmd: verifyCmd, status: checkProc.status });

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

    logger.log('verify_result', {
      status: checkProc.status,
      exitCode: (checkProc as unknown as { exitCode?: number }).exitCode,
      stdout: truncate(stdout.trim(), 500),
      stderr: truncate(stderr.trim(), 500),
    });

    if (stdout.includes('__MISSING__')) {
      return withBundle({ 
        success: false, 
        error: 'Sync aborted: source missing openclaw.json',
        details: 'The local config directory is missing critical files. This could indicate corruption or an incomplete setup.',
      });
    }

    // If we have a non-empty workspace backup, but the local workspace is empty,
    // abort to avoid wiping memory/bootstrap files via `--delete`.
    if (
      stdout.includes('__WORKSPACE_REMOTE_NONEMPTY__') &&
      stdout.includes('__WORKSPACE_LOCAL_EMPTY__')
    ) {
      return withBundle({
        success: false,
        error: 'Sync aborted: local workspace appears empty',
        details:
          'The workspace directory is empty, but an existing workspace backup was found in R2. ' +
          'Aborting to avoid overwriting memory/bootstrap files with an empty workspace.',
      });
    }

    if (!stdout.includes('__OK__')) {
      throw new Error(
        `Timed out waiting for source verification output (status=${checkProc.status}). stdout=${JSON.stringify(stdout.trim())} stderr=${JSON.stringify(stderr.trim())}`
      );
    }
  } catch (err) {
    logger.log('verify_error', { error: err instanceof Error ? err.message : String(err) });
    return withBundle({ 
      success: false, 
      error: 'Failed to verify source files',
      details: err instanceof Error ? err.message : 'Unknown error',
    });
  }

  // Run rsync to backup config to R2
  // Note: Use --no-times because s3fs doesn't support setting timestamps
  //
  // Also include rsync progress/stats so Cloudflare logs show whether we are
  // "slow" vs "stuck". Without this, rsync can spend a long time building file
  // lists with no output.
  const rsyncCommonFlags =
    `-r --no-times --delete --info=progress2 --stats --human-readable ` +
    `--exclude='*.lock' --exclude='*.log' --exclude='*.tmp'`;
  const syncOpenclawCmd = `rsync ${rsyncCommonFlags} ${OPENCLAW_CONFIG_DIR}/ ${R2_MOUNT_PATH}/openclaw/`;
  // Persist OpenClaw workspace (memory + core bootstrap files) to R2.
  // Exclude `skills/` because we sync that separately.
  const syncWorkspaceCmd =
    `rsync ${rsyncCommonFlags} --exclude='skills/' ${OPENCLAW_WORKSPACE_DIR}/ ${R2_MOUNT_PATH}/workspace/`;
  const syncSkillsCmd = `rsync ${rsyncCommonFlags} ${OPENCLAW_WORKSPACE_DIR}/skills/ ${R2_MOUNT_PATH}/skills/`;
  const writeTimestampCmd = `sh -lc 'date -Iseconds > ${R2_MOUNT_PATH}/.last-sync'`;
  
  try {
    logger.log('rsync_started', { cmd: syncOpenclawCmd });
    const proc = await sandbox.startProcess(syncOpenclawCmd);
    logger.log('rsync_process', { status: proc.status });

    try {
      await waitForProcess(proc, DEFAULT_SYNC_TIMEOUT_MS);
    } catch (e) {
      // Include logs/status to help diagnose timeouts or stuck rsync.
      const logs = await proc.getLogs().catch(() => ({ stdout: '', stderr: '' }));
      logger.log('rsync_wait_error', {
        error: e instanceof Error ? e.message : String(e),
        status: proc.status,
        exitCode: (proc as unknown as { exitCode?: number }).exitCode,
        stdout: truncate((logs.stdout || '').trim(), 2000),
        stderr: truncate((logs.stderr || '').trim(), 2000),
      });
      await bestEffortKill(logger.log, proc, 'rsync_openclaw_timeout');
      throw e;
    }
    logger.log('rsync_wait_done', {
      status: proc.status,
      exitCode: (proc as unknown as { exitCode?: number }).exitCode,
    });
    const openclawResult = await ensureRsyncSuccess(logger.log, proc, 'openclaw');
    if (openclawResult) return withBundle(openclawResult);

    logger.log('rsync_started', { cmd: syncWorkspaceCmd });
    const workspaceProc = await sandbox.startProcess(syncWorkspaceCmd);
    logger.log('rsync_process', { status: workspaceProc.status });

    try {
      await waitForProcess(workspaceProc, DEFAULT_SYNC_TIMEOUT_MS);
    } catch (e) {
      const logs = await workspaceProc.getLogs().catch(() => ({ stdout: '', stderr: '' }));
      logger.log('rsync_wait_error', {
        error: e instanceof Error ? e.message : String(e),
        status: workspaceProc.status,
        exitCode: (workspaceProc as unknown as { exitCode?: number }).exitCode,
        stdout: truncate((logs.stdout || '').trim(), 2000),
        stderr: truncate((logs.stderr || '').trim(), 2000),
      });
      await bestEffortKill(logger.log, workspaceProc, 'rsync_workspace_timeout');
      throw e;
    }
    const workspaceResult = await ensureRsyncSuccess(logger.log, workspaceProc, 'workspace');
    if (workspaceResult) return withBundle(workspaceResult);

    logger.log('rsync_started', { cmd: syncSkillsCmd });
    const skillsProc = await sandbox.startProcess(syncSkillsCmd);
    logger.log('rsync_process', { status: skillsProc.status });

    try {
      await waitForProcess(skillsProc, DEFAULT_SYNC_TIMEOUT_MS);
    } catch (e) {
      const logs = await skillsProc.getLogs().catch(() => ({ stdout: '', stderr: '' }));
      logger.log('rsync_wait_error', {
        error: e instanceof Error ? e.message : String(e),
        status: skillsProc.status,
        exitCode: (skillsProc as unknown as { exitCode?: number }).exitCode,
        stdout: truncate((logs.stdout || '').trim(), 2000),
        stderr: truncate((logs.stderr || '').trim(), 2000),
      });
      await bestEffortKill(logger.log, skillsProc, 'rsync_skills_timeout');
      throw e;
    }
    const skillsResult = await ensureRsyncSuccess(logger.log, skillsProc, 'skills');
    if (skillsResult) return withBundle(skillsResult);

    logger.log('timestamp_write_started', { cmd: writeTimestampCmd });
    const writeProc = await sandbox.startProcess(writeTimestampCmd);
    await waitForProcess(writeProc, 15000);

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

    logger.log('timestamp_result', {
      status: timestampProc.status,
      exitCode: (timestampProc as unknown as { exitCode?: number }).exitCode,
      stdout: truncate((timestampLogs.stdout || '').trim(), 500),
      stderr: truncate((timestampLogs.stderr || '').trim(), 500),
    });
    
    if (lastSync && lastSync.match(/^\d{4}-\d{2}-\d{2}/)) {
      return withBundle({ success: true, lastSync });
    } else {
      const logs = await proc.getLogs();
      const diagProc = await sandbox.startProcess(
        // Run under a shell because this command uses `&&`, `||`, and pipes.
        `sh -lc ` +
        `"echo '[diag] mount:' && mount | grep s3fs || true; ` +
        `echo '[diag] r2_path:' && ls -la ${R2_MOUNT_PATH} || true; ` +
        `echo '[diag] openclaw_src:' && ls -la ${OPENCLAW_CONFIG_DIR} || true; ` +
        `echo '[diag] workspace_src:' && ls -la ${OPENCLAW_WORKSPACE_DIR} || true; ` +
        `echo '[diag] skills_src:' && ls -la ${OPENCLAW_WORKSPACE_DIR}/skills || true"`
      );
      await waitForProcess(diagProc, 5000, 200).catch(() => {});
      const diagLogs = await diagProc.getLogs().catch(() => ({ stdout: '', stderr: '' }));
      logger.log('timestamp_missing_diag', {
        rsyncStatus: proc.status,
        rsyncExitCode: (proc as unknown as { exitCode?: number }).exitCode,
        rsyncStdout: truncate((logs.stdout || '').trim(), 2000),
        rsyncStderr: truncate((logs.stderr || '').trim(), 2000),
        diagStdout: truncate((diagLogs.stdout || '').trim(), 2000),
        diagStderr: truncate((diagLogs.stderr || '').trim(), 2000),
      });
      return withBundle({
        success: false,
        error: 'Sync failed',
        details: [
          logs.stderr || logs.stdout || 'No timestamp file created',
          diagLogs.stdout?.trim(),
          diagLogs.stderr?.trim(),
        ].filter(Boolean).join('\n'),
      });
    }
  } catch (err) {
    logger.log('sync_error', { error: err instanceof Error ? err.message : String(err) });
    return withBundle({ 
      success: false, 
      error: 'Sync error',
      details: err instanceof Error ? err.message : 'Unknown error',
    });
  }
}
