import type { Sandbox } from '@cloudflare/sandbox';
import type { OpenClawEnv } from '../types';
import { R2_MOUNT_PATH, R2_BUCKET_NAME } from '../config';
import { waitForProcess } from './utils';

async function probeMountResponsive(sandbox: Sandbox): Promise<boolean> {
  // If the s3fs mount is stale/unresponsive, simple filesystem calls can hang
  // indefinitely. Use a short worker-side timeout to detect this.
  const proc = await sandbox.startProcess(`sh -lc 'ls -la "${R2_MOUNT_PATH}" >/dev/null 2>&1; echo "__OK__"'`);
  try {
    await waitForProcess(proc, 5000, 200);
  } catch {
    return false;
  }
  const logs = await proc.getLogs().catch(() => ({ stdout: '', stderr: '' }));
  return (logs.stdout || '').includes('__OK__');
}

async function lazyUnmountR2(sandbox: Sandbox): Promise<void> {
  // Try a lazy unmount first. This avoids blocking if the mount is hung.
  // Not all images have fusermount(3), so attempt umount first.
  const cmd =
    `sh -lc ` +
    `"umount -l '${R2_MOUNT_PATH}' 2>/dev/null || ` +
    `fusermount -uz '${R2_MOUNT_PATH}' 2>/dev/null || ` +
    `fusermount -u '${R2_MOUNT_PATH}' 2>/dev/null || true"`;
  const proc = await sandbox.startProcess(cmd);
  // Best-effort wait: if unmount hangs, we'll just continue and rely on mountBucket errors.
  await waitForProcess(proc, 5000, 200).catch(() => {});
}

/**
 * Check if R2 is already mounted by looking at the mount table.
 *
 * Important: sandbox process status can lag behind actual completion, so we
 * must not treat a `running` status as authoritative for small commands.
 */
async function isR2Mounted(sandbox: Sandbox): Promise<boolean> {
  try {
    // Prefer /proc/mounts: stable format and no shell needed.
    const proc = await sandbox.startProcess('cat /proc/mounts');

    // Best-effort wait: status may lag, so ignore timeouts.
    await waitForProcess(proc, 5000, 200).catch(() => {});

    // Poll logs briefly in case status/logs lag behind each other.
    let stdout = '';
    for (let i = 0; i < 10; i++) {
      const logs = await proc.getLogs().catch(() => ({ stdout: '', stderr: '' }));
      stdout = logs.stdout || '';
      if (stdout.trim().length > 0) break;
      await new Promise((r) => setTimeout(r, 100));
    }

    const lines = stdout.split('\n');
    // `mount` output uses "on <path>", /proc/mounts uses "<path>".
    const mounted = lines.some((line) => {
      if (!line.includes('s3fs')) return false;
      return line.includes(` ${R2_MOUNT_PATH} `) || line.includes(` on ${R2_MOUNT_PATH} `);
    });

    console.log('isR2Mounted check:', mounted);
    return mounted;
  } catch (err) {
    console.log('isR2Mounted error:', err);
    return false;
  }
}

/**
 * Mount R2 bucket for persistent storage
 * 
 * @param sandbox - The sandbox instance
 * @param env - Worker environment bindings
 * @returns true if mounted successfully, false otherwise
 */
export async function mountR2Storage(sandbox: Sandbox, env: OpenClawEnv): Promise<boolean> {
  // Skip if R2 credentials are not configured
  if (!env.R2_ACCESS_KEY_ID || !env.R2_SECRET_ACCESS_KEY || !env.CF_ACCOUNT_ID) {
    console.log('R2 storage not configured (missing R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, or CF_ACCOUNT_ID)');
    return false;
  }

  // Check if already mounted first - this avoids errors and is faster
  if (await isR2Mounted(sandbox)) {
    // Being "mounted" doesn't mean it's responsive. s3fs mounts can become stale
    // and cause filesystem operations to hang.
    const responsive = await probeMountResponsive(sandbox);
    if (responsive) {
      console.log('R2 bucket already mounted at', R2_MOUNT_PATH);
      return true;
    }

    console.log('R2 bucket mount appears unresponsive, attempting remount at', R2_MOUNT_PATH);
    await lazyUnmountR2(sandbox);
  }

  try {
    console.log('Mounting R2 bucket at', R2_MOUNT_PATH);
    await sandbox.mountBucket(R2_BUCKET_NAME, R2_MOUNT_PATH, {
      endpoint: `https://${env.CF_ACCOUNT_ID}.r2.cloudflarestorage.com`,
      // Pass credentials explicitly since we use R2_* naming instead of AWS_*
      credentials: {
        accessKeyId: env.R2_ACCESS_KEY_ID,
        secretAccessKey: env.R2_SECRET_ACCESS_KEY,
      },
    });
    console.log('R2 bucket mounted successfully - openclaw data will persist across sessions');
    return true;
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    console.log('R2 mount error:', errorMessage);
    
    // Check again if it's mounted - the error might be misleading
    if (await isR2Mounted(sandbox)) {
      // Only accept it as "ok" if the mount is responsive.
      const responsive = await probeMountResponsive(sandbox);
      if (responsive) {
        console.log('R2 bucket is mounted despite error');
        return true;
      }
      console.log('R2 bucket is mounted but unresponsive after error');
    }
    
    // Don't fail if mounting fails - openclaw can still run without persistent storage
    console.error('Failed to mount R2 bucket:', err);
    return false;
  }
}
