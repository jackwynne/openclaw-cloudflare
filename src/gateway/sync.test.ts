import { describe, it, expect, vi, beforeEach } from 'vitest';
import { syncToR2 } from './sync';
import { 
  createMockEnv, 
  createMockEnvWithR2, 
  createMockProcess, 
  createMockSandbox, 
  suppressConsole 
} from '../test-utils';

describe('syncToR2', () => {
  beforeEach(() => {
    suppressConsole();
  });

  describe('configuration checks', () => {
    it('returns error when R2 is not configured', async () => {
      const { sandbox } = createMockSandbox();
      const env = createMockEnv();

      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(false);
      expect(result.error).toBe('R2 storage is not configured');
    });

    it('returns error when mount fails', async () => {
      const { sandbox, startProcessMock, mountBucketMock } = createMockSandbox();
      startProcessMock.mockResolvedValue(createMockProcess(''));
      mountBucketMock.mockRejectedValue(new Error('Mount failed'));
      
      const env = createMockEnvWithR2();

      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(false);
      expect(result.error).toBe('Failed to mount R2 storage');
    });
  });

  describe('sanity checks', () => {
    it('returns error when source is missing openclaw.json', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();
      startProcessMock
        .mockResolvedValueOnce(createMockProcess('fuse.s3fs\n')) // r2 probe says mounted
        .mockResolvedValueOnce(createMockProcess('__MISSING__')); // Missing marker output
      
      const env = createMockEnvWithR2();

      const result = await syncToR2(sandbox, env);

      // Error message still references openclaw.json since that's the actual file name
      expect(result.success).toBe(false);
      expect(result.error).toBe('Sync aborted: source missing openclaw.json');
      expect(result.details).toContain('missing critical files');
    });
  });

  describe('sync execution', () => {
    it('returns success when sync completes', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();
      const timestamp = '2026-01-27T12:00:00+00:00';
      
      // Calls:
      // 1) r2 probe (stat -f)
      // 2) verify
      // 3) rsync openclaw
      // 4) rsync workspace
      // 5) rsync skills
      // 6) write timestamp
      // 7) read timestamp
      startProcessMock
        .mockResolvedValueOnce(createMockProcess('fuse.s3fs\n'))
        .mockResolvedValueOnce(createMockProcess('__OK__'))
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess(timestamp));
      
      const env = createMockEnvWithR2();

      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(true);
      expect(result.lastSync).toBe(timestamp);
    });

    it('returns error when rsync fails (no timestamp created)', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();
      
      // Calls: isR2Mounted, r2 mount probe, verify, rsync openclaw (fails)
      startProcessMock
        .mockResolvedValueOnce(createMockProcess('fuse.s3fs\n'))
        .mockResolvedValueOnce(createMockProcess('__OK__'))
        .mockResolvedValueOnce(createMockProcess('', { exitCode: 1 }));
      
      const env = createMockEnvWithR2();

      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(false);
      expect(result.error).toBe('Sync failed');
    });

    it('verifies rsync command is called with correct flags', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();
      const timestamp = '2026-01-27T12:00:00+00:00';
      
      startProcessMock
        .mockResolvedValueOnce(createMockProcess('fuse.s3fs\n'))
        .mockResolvedValueOnce(createMockProcess('__OK__'))
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess(timestamp));
      
      const env = createMockEnvWithR2();

      await syncToR2(sandbox, env);

      // Third call should be rsync openclaw (paths still use openclaw internally)
      const rsyncCall = startProcessMock.mock.calls[2][0];
      expect(rsyncCall).toContain('rsync');
      expect(rsyncCall).toContain('--no-times');
      expect(rsyncCall).toContain('--delete');
      expect(rsyncCall).toContain('/root/.openclaw/');
      expect(rsyncCall).toContain('/data/openclaw/');
      expect(rsyncCall).toContain('--info=progress2');
    });
  });
});
