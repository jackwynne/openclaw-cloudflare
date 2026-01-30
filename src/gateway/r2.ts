import type { Sandbox } from '@cloudflare/sandbox';
import type { OpenClawEnv } from '../types';
import { R2_MOUNT_PATH, R2_BUCKET_NAME } from '../config';
import { waitForProcess } from './utils';

type MountProbe =
  | { state: 'mounted'; fsType: string }
  | { state: 'not_mounted'; fsType: string }
  | { state: 'unresponsive' };

async function probeMount(sandbox: Sandbox): Promise<MountProbe> {
  // Use filesystem type instead of /proc/mounts parsing. In Cloudflare Sandbox,
  // mountBucket() can report "path in use" even when /proc/mounts doesn't show
  // an obvious s3fs entry.
  //
  // Also: if the mount is stale/unresponsive, stat can hang. Treat that as
  // unresponsive and trigger a remount attempt.
  const proc = await sandbox.startProcess(
    `sh -lc 'stat -f -c %T "${R2_MOUNT_PATH}" 2>/dev/null || echo "__ERR__"'`
  );
  try {
    await waitForProcess(proc, 5000, 200);
  } catch {
    return { state: 'unresponsive' };
  }

  const logs = await proc.getLogs().catch(() => ({ stdout: '', stderr: '' }));
  const fsType = (logs.stdout || '').trim().split(/\s+/)[0] || '';
  if (!fsType || fsType === '__ERR__') return { state: 'not_mounted', fsType: fsType || '__ERR__' };

  // When mounted via mountBucket, this is typically a fuse filesystem.
  if (fsType.startsWith('fuse')) return { state: 'mounted', fsType };
  return { state: 'not_mounted', fsType };
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

function isMountPathInUseError(errorMessage: string): boolean {
  return (
    errorMessage.includes('InvalidMountConfigError') &&
    errorMessage.includes('Mount path') &&
    errorMessage.includes('already in use')
  );
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

  // Check mount state up front. This is both faster and avoids InvalidMountConfigError.
  const initialProbe = await probeMount(sandbox);
  if (initialProbe.state === 'mounted') {
    console.log('R2 bucket already mounted at', R2_MOUNT_PATH, '(fsType:', initialProbe.fsType + ')');
    return true;
  }
  if (initialProbe.state === 'unresponsive') {
    console.log('R2 mount appears unresponsive, attempting remount at', R2_MOUNT_PATH);
    await lazyUnmountR2(sandbox);
  } else {
    console.log('R2 mount not detected at', R2_MOUNT_PATH, '(fsType:', initialProbe.fsType + ')');
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

    // If the platform reports "path already in use", treat it as already-mounted
    // unless the mount is unresponsive.
    if (isMountPathInUseError(errorMessage)) {
      const probe = await probeMount(sandbox);
      if (probe.state === 'mounted') {
        console.log('R2 mount path is in use and appears mounted (fsType:', probe.fsType + ')');
        return true;
      }
      if (probe.state === 'unresponsive') {
        console.log('R2 mount path is in use but unresponsive; attempting remount at', R2_MOUNT_PATH);
        await lazyUnmountR2(sandbox);
        try {
          await sandbox.mountBucket(R2_BUCKET_NAME, R2_MOUNT_PATH, {
            endpoint: `https://${env.CF_ACCOUNT_ID}.r2.cloudflarestorage.com`,
            credentials: {
              accessKeyId: env.R2_ACCESS_KEY_ID,
              secretAccessKey: env.R2_SECRET_ACCESS_KEY,
            },
          });
          console.log('R2 bucket mounted successfully after remount attempt');
          return true;
        } catch (retryErr) {
          console.log('R2 remount error:', retryErr instanceof Error ? retryErr.message : String(retryErr));
        }
      }
    }
    
    // Don't fail if mounting fails - openclaw can still run without persistent storage
    console.error('Failed to mount R2 bucket:', err);
    return false;
  }
}
