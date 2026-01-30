/**
 * Shared utilities for gateway operations
 */

/**
 * Wait for a sandbox process to complete
 * 
 * @param proc - Process object with status property
 * @param timeoutMs - Maximum time to wait in milliseconds
 * @param pollIntervalMs - How often to check status (default 500ms)
 */
export async function waitForProcess(
  proc: { status: string }, 
  timeoutMs: number,
  pollIntervalMs: number = 500
): Promise<void> {
  const maxAttempts = Math.ceil(timeoutMs / pollIntervalMs);
  let attempts = 0;
  // Sandbox processes often begin as "starting" before transitioning to "running".
  // If we only wait on "running", we can exit immediately and race subsequent checks.
  while ((proc.status === 'starting' || proc.status === 'running') && attempts < maxAttempts) {
    await new Promise(r => setTimeout(r, pollIntervalMs));
    attempts++;
  }

  // If we exhausted attempts but the process is still active, treat it as a timeout.
  if (proc.status === 'starting' || proc.status === 'running') {
    throw new Error(`Process timed out after ${timeoutMs}ms (status=${proc.status})`);
  }
}
