/**
 * Browser SDK Tests
 *
 * Tests for @dotdo/events Browser SDK
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import type { BrowserConfig } from '../browser.js'

// ============================================================================
// Browser Globals Mock Setup
// ============================================================================

function createMockStorage(): Storage {
  const store = new Map<string, string>()
  return {
    getItem: vi.fn((key: string) => store.get(key) ?? null),
    setItem: vi.fn((key: string, value: string) => store.set(key, value)),
    removeItem: vi.fn((key: string) => store.delete(key)),
    clear: vi.fn(() => store.clear()),
    key: vi.fn((index: number) => [...store.keys()][index] ?? null),
    get length() { return store.size },
  }
}

function createMockDocument() {
  return {
    referrer: 'https://google.com',
    visibilityState: 'visible' as DocumentVisibilityState,
  }
}

function createMockNavigator() {
  return {
    userAgent: 'Mozilla/5.0 (Test Browser)',
    sendBeacon: vi.fn(() => true),
  }
}

function createMockLocation() {
  return {
    href: 'https://example.com/page?foo=bar',
    pathname: '/page',
  }
}

// ============================================================================
// Tests
// ============================================================================

describe('Browser SDK', () => {
  let mockFetch: ReturnType<typeof vi.fn>
  let mockLocalStorage: Storage
  let mockSessionStorage: Storage
  let mockDocument: ReturnType<typeof createMockDocument>
  let mockNavigator: ReturnType<typeof createMockNavigator>
  let mockLocation: ReturnType<typeof createMockLocation>
  let addEventListenerSpy: ReturnType<typeof vi.fn>
  let eventListeners: Map<string, EventListener[]>

  beforeEach(async () => {
    vi.useFakeTimers()

    // Set up browser globals
    mockFetch = vi.fn().mockResolvedValue(new Response('OK', { status: 200 }))
    mockLocalStorage = createMockStorage()
    mockSessionStorage = createMockStorage()
    mockDocument = createMockDocument()
    mockNavigator = createMockNavigator()
    mockLocation = createMockLocation()
    eventListeners = new Map()

    addEventListenerSpy = vi.fn((event: string, handler: EventListener) => {
      if (!eventListeners.has(event)) {
        eventListeners.set(event, [])
      }
      eventListeners.get(event)!.push(handler)
    })

    vi.stubGlobal('fetch', mockFetch)
    vi.stubGlobal('localStorage', mockLocalStorage)
    vi.stubGlobal('sessionStorage', mockSessionStorage)
    vi.stubGlobal('document', mockDocument)
    vi.stubGlobal('navigator', mockNavigator)
    vi.stubGlobal('location', mockLocation)
    vi.stubGlobal('addEventListener', addEventListenerSpy)
    vi.stubGlobal('window', {})

    // Reset module cache to get fresh SDK state
    vi.resetModules()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  describe('EventsSDK constructor', () => {
    it('should use default config values', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      // Verify localStorage/sessionStorage accessed for IDs
      expect(mockLocalStorage.getItem).toHaveBeenCalledWith('events_a')
      expect(mockSessionStorage.getItem).toHaveBeenCalledWith('events_s')
    })

    it('should accept custom endpoint', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK({ endpoint: '/custom-endpoint' })

      sdk.track('test')
      await sdk.flush()

      expect(mockFetch).toHaveBeenCalledWith(
        '/custom-endpoint',
        expect.any(Object)
      )
    })

    it('should accept custom batch size', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK({ batchSize: 2 })

      sdk.track('event1')
      expect(mockFetch).not.toHaveBeenCalled()

      sdk.track('event2')
      // Auto-flush should trigger when batch size reached
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('should set up flush interval timer', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK({ flushInterval: 1000 })

      sdk.track('test')
      expect(mockFetch).not.toHaveBeenCalled()

      await vi.advanceTimersByTimeAsync(1000)
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('should register visibility change listener', async () => {
      const { EventsSDK } = await import('../browser.js')
      new EventsSDK()

      expect(addEventListenerSpy).toHaveBeenCalledWith(
        'visibilitychange',
        expect.any(Function)
      )
    })

    it('should register pagehide listener', async () => {
      const { EventsSDK } = await import('../browser.js')
      new EventsSDK()

      expect(addEventListenerSpy).toHaveBeenCalledWith(
        'pagehide',
        expect.any(Function)
      )
    })

    it('should generate anonymous ID if not in localStorage', async () => {
      const { EventsSDK } = await import('../browser.js')
      new EventsSDK()

      expect(mockLocalStorage.setItem).toHaveBeenCalledWith(
        'events_a',
        expect.any(String)
      )
    })

    it('should reuse existing anonymous ID from localStorage', async () => {
      mockLocalStorage.setItem('events_a', 'existing-anon-id')
      ;(mockLocalStorage.getItem as ReturnType<typeof vi.fn>).mockImplementation(
        (key: string) => key === 'events_a' ? 'existing-anon-id' : null
      )

      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.track('test')
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].anonymousId).toBe('existing-anon-id')
    })

    it('should generate session ID if not in sessionStorage', async () => {
      const { EventsSDK } = await import('../browser.js')
      new EventsSDK()

      expect(mockSessionStorage.setItem).toHaveBeenCalledWith(
        'events_s',
        expect.any(String)
      )
    })
  })

  describe('page()', () => {
    it('should track page view event', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.page()
      await sdk.flush()

      expect(mockFetch).toHaveBeenCalledTimes(1)
      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events).toHaveLength(1)
      expect(body.events[0].type).toBe('page')
    })

    it('should include URL and path', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.page()
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].url).toBe('https://example.com/page?foo=bar')
      expect(body.events[0].path).toBe('/page')
    })

    it('should include referrer', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.page()
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].referrer).toBe('https://google.com')
    })

    it('should include user agent', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.page()
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].ua).toBe('Mozilla/5.0 (Test Browser)')
    })

    it('should accept custom properties', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.page({ title: 'Home Page', category: 'landing' })
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].properties).toEqual({
        title: 'Home Page',
        category: 'landing',
      })
    })

    it('should include timestamp', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      vi.setSystemTime(new Date('2024-06-15T12:00:00Z'))
      sdk.page()
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].ts).toBe('2024-06-15T12:00:00.000Z')
    })

    it('should include anonymous and session IDs', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.page()
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].anonymousId).toBeDefined()
      expect(body.events[0].sessionId).toBeDefined()
    })
  })

  describe('track()', () => {
    it('should track custom event', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.track('button_click')
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].type).toBe('track')
      expect(body.events[0].event).toBe('button_click')
    })

    it('should include event properties', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.track('purchase', { amount: 99.99, currency: 'USD' })
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].properties).toEqual({
        amount: 99.99,
        currency: 'USD',
      })
    })

    it('should queue multiple events', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.track('event1')
      sdk.track('event2')
      sdk.track('event3')
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events).toHaveLength(3)
    })

    it('should include context in each event', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.track('test_event')
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      const event = body.events[0]
      expect(event.url).toBeDefined()
      expect(event.path).toBeDefined()
      expect(event.referrer).toBeDefined()
      expect(event.ua).toBeDefined()
      expect(event.anonymousId).toBeDefined()
      expect(event.sessionId).toBeDefined()
    })
  })

  describe('identify()', () => {
    it('should set user ID for future events', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.identify('user-123')
      sdk.track('test')
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      // First event is identify, second is track
      expect(body.events[0].type).toBe('identify')
      expect(body.events[0].userId).toBe('user-123')
      expect(body.events[1].userId).toBe('user-123')
    })

    it('should emit identify event', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.identify('user-456')
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].type).toBe('identify')
      expect(body.events[0].userId).toBe('user-456')
    })

    it('should include user traits', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.identify('user-789', { email: 'test@example.com', plan: 'pro' })
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].traits).toEqual({
        email: 'test@example.com',
        plan: 'pro',
      })
    })

    it('should persist user ID across subsequent calls', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.identify('persistent-user')
      sdk.page()
      sdk.track('action1')
      sdk.track('action2')
      await sdk.flush()

      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      body.events.forEach((event: { userId: string }) => {
        expect(event.userId).toBe('persistent-user')
      })
    })
  })

  describe('flush()', () => {
    it('should send batched events to endpoint', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.track('event1')
      sdk.track('event2')
      await sdk.flush()

      expect(mockFetch).toHaveBeenCalledWith(
        '/e',
        expect.objectContaining({
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          keepalive: true,
        })
      )
    })

    it('should clear queue after flush', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.track('event1')
      await sdk.flush()
      await sdk.flush()

      // Second flush should not send anything
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('should not send if queue is empty', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      await sdk.flush()

      expect(mockFetch).not.toHaveBeenCalled()
    })

    it('should use sendBeacon when beacon flag is set', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.track('event1')
      await sdk.flush(1)

      expect(mockNavigator.sendBeacon).toHaveBeenCalledWith(
        '/e',
        expect.any(String)
      )
      expect(mockFetch).not.toHaveBeenCalled()
    })

    it('should fall back to fetch if sendBeacon unavailable', async () => {
      const navigatorWithoutBeacon = {
        userAgent: 'Mozilla/5.0 (Test Browser)',
        sendBeacon: undefined,
      }
      vi.stubGlobal('navigator', navigatorWithoutBeacon)
      vi.resetModules()

      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.track('event1')
      await sdk.flush(1)

      expect(mockFetch).toHaveBeenCalled()
    })

    it('should handle fetch errors gracefully', async () => {
      mockFetch.mockRejectedValueOnce(new Error('Network error'))

      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.track('event1')

      // Should not throw
      await expect(sdk.flush()).resolves.not.toThrow()
    })
  })

  describe('auto-flush on visibility change', () => {
    it('should flush when document becomes hidden', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.track('event1')

      // Simulate visibility change to hidden
      mockDocument.visibilityState = 'hidden'
      const visibilityHandlers = eventListeners.get('visibilitychange') || []
      visibilityHandlers.forEach(handler => handler(new Event('visibilitychange')))

      expect(mockNavigator.sendBeacon).toHaveBeenCalled()
    })

    it('should not flush when document becomes visible', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.track('event1')

      // Simulate visibility change to visible
      mockDocument.visibilityState = 'visible'
      const visibilityHandlers = eventListeners.get('visibilitychange') || []
      visibilityHandlers.forEach(handler => handler(new Event('visibilitychange')))

      expect(mockNavigator.sendBeacon).not.toHaveBeenCalled()
      expect(mockFetch).not.toHaveBeenCalled()
    })

    it('should flush on pagehide', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK()

      sdk.track('event1')

      // Simulate pagehide
      const pagehideHandlers = eventListeners.get('pagehide') || []
      pagehideHandlers.forEach(handler => handler(new Event('pagehide')))

      expect(mockNavigator.sendBeacon).toHaveBeenCalled()
    })
  })

  describe('destroy()', () => {
    it('should clear interval and flush remaining events', async () => {
      const { EventsSDK } = await import('../browser.js')
      const sdk = new EventsSDK({ flushInterval: 5000 })

      sdk.track('event1')
      sdk.destroy()

      expect(mockNavigator.sendBeacon).toHaveBeenCalled()
    })
  })

  describe('module-level functions', () => {
    it('init() should create and return SDK instance', async () => {
      const { init } = await import('../browser.js')

      const sdk = init({ endpoint: '/test' })
      expect(sdk).toBeDefined()
    })

    it('init() should return same instance on subsequent calls', async () => {
      const { init } = await import('../browser.js')

      const sdk1 = init({ endpoint: '/first' })
      const sdk2 = init({ endpoint: '/second' }) // Should be ignored

      expect(sdk1).toBe(sdk2)
    })

    it('page() should work without explicit init', async () => {
      const { page, flush } = await import('../browser.js')

      page()
      await flush()

      expect(mockFetch).toHaveBeenCalled()
      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].type).toBe('page')
    })

    it('track() should work without explicit init', async () => {
      const { track, flush } = await import('../browser.js')

      track('test_event', { key: 'value' })
      await flush()

      expect(mockFetch).toHaveBeenCalled()
      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].type).toBe('track')
      expect(body.events[0].event).toBe('test_event')
    })

    it('identify() should work without explicit init', async () => {
      const { identify, flush } = await import('../browser.js')

      identify('user-123', { name: 'Test' })
      await flush()

      expect(mockFetch).toHaveBeenCalled()
      const body = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(body.events[0].type).toBe('identify')
    })

    it('flush() should resolve if no SDK initialized', async () => {
      vi.resetModules()
      // Re-stub globals after reset
      vi.stubGlobal('fetch', mockFetch)
      vi.stubGlobal('localStorage', mockLocalStorage)
      vi.stubGlobal('sessionStorage', mockSessionStorage)
      vi.stubGlobal('document', mockDocument)
      vi.stubGlobal('navigator', mockNavigator)
      vi.stubGlobal('location', mockLocation)
      vi.stubGlobal('addEventListener', addEventListenerSpy)
      vi.stubGlobal('window', undefined)

      const { flush } = await import('../browser.js')

      await expect(flush()).resolves.toBeUndefined()
    })
  })

  describe('edge cases', () => {
    describe('localStorage unavailable', () => {
      it('should fallback to generated ID when localStorage throws', async () => {
        const failingStorage = {
          getItem: vi.fn(() => { throw new Error('Storage disabled') }),
          setItem: vi.fn(() => { throw new Error('Storage disabled') }),
          removeItem: vi.fn(),
          clear: vi.fn(),
          key: vi.fn(),
          length: 0,
        }
        vi.stubGlobal('localStorage', failingStorage)
        vi.resetModules()

        // Re-stub other globals after reset
        vi.stubGlobal('fetch', mockFetch)
        vi.stubGlobal('sessionStorage', mockSessionStorage)
        vi.stubGlobal('document', mockDocument)
        vi.stubGlobal('navigator', mockNavigator)
        vi.stubGlobal('location', mockLocation)
        vi.stubGlobal('addEventListener', addEventListenerSpy)
        vi.stubGlobal('window', {})

        const { EventsSDK } = await import('../browser.js')

        // Should not throw
        const sdk = new EventsSDK()
        sdk.track('test')
        await sdk.flush()

        const body = JSON.parse(mockFetch.mock.calls[0][1].body)
        expect(body.events[0].anonymousId).toBeDefined()
        expect(typeof body.events[0].anonymousId).toBe('string')
      })
    })

    describe('sessionStorage unavailable', () => {
      it('should fallback to generated ID when sessionStorage throws', async () => {
        const failingStorage = {
          getItem: vi.fn(() => { throw new Error('Storage disabled') }),
          setItem: vi.fn(() => { throw new Error('Storage disabled') }),
          removeItem: vi.fn(),
          clear: vi.fn(),
          key: vi.fn(),
          length: 0,
        }
        vi.stubGlobal('sessionStorage', failingStorage)
        vi.resetModules()

        // Re-stub other globals after reset
        vi.stubGlobal('fetch', mockFetch)
        vi.stubGlobal('localStorage', mockLocalStorage)
        vi.stubGlobal('document', mockDocument)
        vi.stubGlobal('navigator', mockNavigator)
        vi.stubGlobal('location', mockLocation)
        vi.stubGlobal('addEventListener', addEventListenerSpy)
        vi.stubGlobal('window', {})

        const { EventsSDK } = await import('../browser.js')

        // Should not throw
        const sdk = new EventsSDK()
        sdk.track('test')
        await sdk.flush()

        const body = JSON.parse(mockFetch.mock.calls[0][1].body)
        expect(body.events[0].sessionId).toBeDefined()
        expect(typeof body.events[0].sessionId).toBe('string')
      })
    })

    describe('server-side rendering (no window)', () => {
      it('should not set up timers when window is undefined', async () => {
        vi.stubGlobal('window', undefined)
        vi.resetModules()

        // Re-stub other globals after reset
        vi.stubGlobal('fetch', mockFetch)
        vi.stubGlobal('localStorage', mockLocalStorage)
        vi.stubGlobal('sessionStorage', mockSessionStorage)
        vi.stubGlobal('document', mockDocument)
        vi.stubGlobal('navigator', mockNavigator)
        vi.stubGlobal('location', mockLocation)
        vi.stubGlobal('addEventListener', addEventListenerSpy)

        const { EventsSDK } = await import('../browser.js')

        // Should not throw
        const sdk = new EventsSDK({ flushInterval: 1000 })
        sdk.track('test')

        // Advance time - no auto-flush should occur since no timer was set
        // (We're testing that it doesn't error when window is undefined)
        await vi.advanceTimersByTimeAsync(2000)

        // Manual flush should still work
        await sdk.flush()
        expect(mockFetch).toHaveBeenCalled()
      })
    })

    describe('rapid fire events', () => {
      it('should handle many events in quick succession', async () => {
        const { EventsSDK } = await import('../browser.js')
        const sdk = new EventsSDK({ batchSize: 100 })

        // Fire 50 events rapidly
        for (let i = 0; i < 50; i++) {
          sdk.track(`event_${i}`, { index: i })
        }

        await sdk.flush()

        const body = JSON.parse(mockFetch.mock.calls[0][1].body)
        expect(body.events).toHaveLength(50)
      })

      it('should auto-flush at batch size limit', async () => {
        const { EventsSDK } = await import('../browser.js')
        const sdk = new EventsSDK({ batchSize: 5 })

        // Fire exactly 5 events to trigger auto-flush
        for (let i = 0; i < 5; i++) {
          sdk.track(`event_${i}`)
        }

        // First batch should have auto-flushed
        expect(mockFetch).toHaveBeenCalledTimes(1)

        // Fire 3 more
        for (let i = 0; i < 3; i++) {
          sdk.track(`event_second_${i}`)
        }

        await sdk.flush()

        // Second batch should be flushed manually
        expect(mockFetch).toHaveBeenCalledTimes(2)
        const body = JSON.parse(mockFetch.mock.calls[1][1].body)
        expect(body.events).toHaveLength(3)
      })
    })

    describe('offline behavior', () => {
      it('should handle network errors silently', async () => {
        mockFetch.mockRejectedValue(new Error('Failed to fetch'))

        const { EventsSDK } = await import('../browser.js')
        const sdk = new EventsSDK()

        sdk.track('offline_event')

        // Should not throw
        await expect(sdk.flush()).resolves.not.toThrow()
      })
    })
  })
})
