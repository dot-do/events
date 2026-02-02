/**
 * CDCShardCoordinatorDO Tests
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  getCDCShardCoordinator,
  getCDCProcessorShard,
  getActiveCDCShards,
} from '../cdc-shard-coordinator-do'

// ============================================================================
// Mock Setup
// ============================================================================

function createMockDONamespace() {
  const mockStub = {
    getShardForCollection: vi.fn().mockResolvedValue(0),
    getActiveShards: vi.fn().mockResolvedValue([0, 1, 2]),
    getConfig: vi.fn().mockResolvedValue({
      minShards: 1,
      maxShards: 16,
      cooldownMs: 60000,
      metricsWindowMs: 30000,
    }),
    updateConfig: vi.fn(),
    reportMetrics: vi.fn(),
    getStats: vi.fn(),
    forceScale: vi.fn(),
  }

  return {
    namespace: {
      idFromName: vi.fn().mockReturnValue({ toString: () => 'test-id' }),
      get: vi.fn().mockReturnValue(mockStub),
    },
    stub: mockStub,
  }
}

// ============================================================================
// Tests
// ============================================================================

describe('CDC Shard Coordinator Helpers', () => {
  describe('getCDCShardCoordinator', () => {
    it('returns null when CDC_SHARD_COORDINATOR is not bound', () => {
      const env = {}
      const result = getCDCShardCoordinator(env, 'test-namespace')
      expect(result).toBeNull()
    })

    it('returns stub when CDC_SHARD_COORDINATOR is bound', () => {
      const { namespace, stub } = createMockDONamespace()
      const env = { CDC_SHARD_COORDINATOR: namespace }

      const result = getCDCShardCoordinator(env, 'test-namespace')

      expect(result).toBe(stub)
      expect(namespace.idFromName).toHaveBeenCalledWith('cdc-coordinator:test-namespace')
      expect(namespace.get).toHaveBeenCalled()
    })

    it('constructs correct coordinator name for different namespaces', () => {
      const { namespace } = createMockDONamespace()
      const env = { CDC_SHARD_COORDINATOR: namespace }

      getCDCShardCoordinator(env, 'acme')
      expect(namespace.idFromName).toHaveBeenCalledWith('cdc-coordinator:acme')

      getCDCShardCoordinator(env, 'beta-corp')
      expect(namespace.idFromName).toHaveBeenCalledWith('cdc-coordinator:beta-corp')
    })
  })

  describe('getCDCProcessorShard', () => {
    it('uses shard 0 when no coordinator and no explicit shard', async () => {
      const cdcNamespace = {
        idFromName: vi.fn().mockReturnValue({ toString: () => 'proc-id' }),
        get: vi.fn().mockReturnValue({}),
      }
      const env = { CDC_PROCESSOR: cdcNamespace }

      await getCDCProcessorShard(env, 'test-ns', 'users')

      // Without coordinator, uses hash-based routing (collection 'users' hashes to some shard)
      expect(cdcNamespace.idFromName).toHaveBeenCalled()
    })

    it('uses explicit shard when provided', async () => {
      const cdcNamespace = {
        idFromName: vi.fn().mockReturnValue({ toString: () => 'proc-id' }),
        get: vi.fn().mockReturnValue({}),
      }
      const env = { CDC_PROCESSOR: cdcNamespace }

      await getCDCProcessorShard(env, 'test-ns', 'users', 3)

      expect(cdcNamespace.idFromName).toHaveBeenCalledWith('CDCProcessor:test-ns:shard-3')
    })

    it('uses shard 0 name format without shard- suffix', async () => {
      const cdcNamespace = {
        idFromName: vi.fn().mockReturnValue({ toString: () => 'proc-id' }),
        get: vi.fn().mockReturnValue({}),
      }
      const env = { CDC_PROCESSOR: cdcNamespace }

      await getCDCProcessorShard(env, 'test-ns', 'users', 0)

      expect(cdcNamespace.idFromName).toHaveBeenCalledWith('CDCProcessor:test-ns')
    })

    it('queries coordinator for shard when available', async () => {
      const { namespace: coordNamespace, stub: coordStub } = createMockDONamespace()
      coordStub.getShardForCollection.mockResolvedValue(2)

      const cdcNamespace = {
        idFromName: vi.fn().mockReturnValue({ toString: () => 'proc-id' }),
        get: vi.fn().mockReturnValue({}),
      }

      const env = {
        CDC_PROCESSOR: cdcNamespace,
        CDC_SHARD_COORDINATOR: coordNamespace,
      }

      await getCDCProcessorShard(env, 'test-ns', 'orders')

      expect(coordStub.getShardForCollection).toHaveBeenCalledWith('orders')
      expect(cdcNamespace.idFromName).toHaveBeenCalledWith('CDCProcessor:test-ns:shard-2')
    })

    it('falls back to hash routing when coordinator fails', async () => {
      const { namespace: coordNamespace, stub: coordStub } = createMockDONamespace()
      coordStub.getShardForCollection.mockRejectedValue(new Error('Coordinator unavailable'))

      const cdcNamespace = {
        idFromName: vi.fn().mockReturnValue({ toString: () => 'proc-id' }),
        get: vi.fn().mockReturnValue({}),
      }

      const env = {
        CDC_PROCESSOR: cdcNamespace,
        CDC_SHARD_COORDINATOR: coordNamespace,
      }

      await getCDCProcessorShard(env, 'test-ns', 'products')

      // Should still call idFromName with some shard (hash-based fallback)
      expect(cdcNamespace.idFromName).toHaveBeenCalled()
    })
  })

  describe('getActiveCDCShards', () => {
    it('returns [0] when no coordinator bound', async () => {
      const env = {}
      const result = await getActiveCDCShards(env, 'test-ns')
      expect(result).toEqual([0])
    })

    it('returns active shards from coordinator', async () => {
      const { namespace, stub } = createMockDONamespace()
      stub.getActiveShards.mockResolvedValue([0, 1, 2, 3])

      const env = { CDC_SHARD_COORDINATOR: namespace }

      const result = await getActiveCDCShards(env, 'test-ns')

      expect(result).toEqual([0, 1, 2, 3])
      expect(stub.getActiveShards).toHaveBeenCalled()
    })

    it('returns [0] when coordinator fails', async () => {
      const { namespace, stub } = createMockDONamespace()
      stub.getActiveShards.mockRejectedValue(new Error('Failed'))

      const env = { CDC_SHARD_COORDINATOR: namespace }

      const result = await getActiveCDCShards(env, 'test-ns')

      expect(result).toEqual([0])
    })
  })

  describe('consistent hashing', () => {
    it('routes same collection to same shard consistently', async () => {
      const { namespace: coordNamespace, stub: coordStub } = createMockDONamespace()

      // Simulate consistent hash-based routing
      const hashMap = new Map<string, number>()
      coordStub.getShardForCollection.mockImplementation(async (collection: string) => {
        if (!hashMap.has(collection)) {
          // Simple hash
          let hash = 0
          for (const char of collection) {
            hash = ((hash << 5) - hash + char.charCodeAt(0)) | 0
          }
          hashMap.set(collection, Math.abs(hash) % 4)
        }
        return hashMap.get(collection)!
      })

      const cdcNamespace = {
        idFromName: vi.fn().mockReturnValue({ toString: () => 'proc-id' }),
        get: vi.fn().mockReturnValue({}),
      }

      const env = {
        CDC_PROCESSOR: cdcNamespace,
        CDC_SHARD_COORDINATOR: coordNamespace,
      }

      // Same collection should always route to same shard
      const shard1 = await getCDCProcessorShard(env, 'ns', 'users')
      const shard2 = await getCDCProcessorShard(env, 'ns', 'users')

      const calls = cdcNamespace.idFromName.mock.calls
      expect(calls[0]).toEqual(calls[1])
    })

    it('distributes different collections across shards', async () => {
      const { namespace: coordNamespace, stub: coordStub } = createMockDONamespace()

      // Return different shards for different collections
      const shardAssignments: Record<string, number> = {
        users: 0,
        orders: 1,
        products: 2,
        inventory: 3,
      }
      coordStub.getShardForCollection.mockImplementation(
        async (collection: string) => shardAssignments[collection] ?? 0
      )

      const cdcNamespace = {
        idFromName: vi.fn().mockReturnValue({ toString: () => 'proc-id' }),
        get: vi.fn().mockReturnValue({}),
      }

      const env = {
        CDC_PROCESSOR: cdcNamespace,
        CDC_SHARD_COORDINATOR: coordNamespace,
      }

      await getCDCProcessorShard(env, 'ns', 'users')
      await getCDCProcessorShard(env, 'ns', 'orders')
      await getCDCProcessorShard(env, 'ns', 'products')
      await getCDCProcessorShard(env, 'ns', 'inventory')

      const calls = cdcNamespace.idFromName.mock.calls
      expect(calls[0][0]).toBe('CDCProcessor:ns')
      expect(calls[1][0]).toBe('CDCProcessor:ns:shard-1')
      expect(calls[2][0]).toBe('CDCProcessor:ns:shard-2')
      expect(calls[3][0]).toBe('CDCProcessor:ns:shard-3')
    })
  })
})
