import { readFileSync } from 'node:fs'

import { describe, expect, it, vi } from 'vitest'

import { AirQualitySensor } from '../devices/airqualitysensor.js'

const hapSource = readFileSync(new URL('../devices/airqualitysensor.ts', import.meta.url), 'utf8')
const matterSource = readFileSync(new URL('../devices/airqualitysensormatter.ts', import.meta.url), 'utf8')

/**
 * HAP used to `debugSuccessLog` the full provider URL (and the AirNow zip
 * fallback URL), which includes `API_KEY` / `token`. Matter never logged that
 * URL. Keep both pollers the same so a debug log cannot leak the key.
 */
describe('provider request URL logging', () => {
  it('does not log the HAP request URL', () => {
    expect(hapSource).not.toMatch(/debugSuccessLog\(`url:/)
    expect(hapSource).not.toContain('Fallback URL:')
  })

  it('does not log the Matter request URL', () => {
    expect(matterSource).not.toMatch(/debugSuccessLog\(`url:/)
    expect(matterSource).not.toContain('Fallback URL:')
  })

  it('keeps the API key out of debug lines during a HAP refresh', async () => {
    const apiKey = 'secret-api-key-do-not-log'
    const logs: string[] = []
    const record = (...args: unknown[]) => {
      logs.push(args.map(String).join(' '))
    }

    const sensor = {
      SensorUpdateInProgress: false,
      lastResponseData: null,
      lastRequestTime: 0,
      cacheMaxAge: 600_000,
      apiCallCount: 0,
      apiCallResetTime: Date.now() + 3_600_000,
      device: { provider: 'airnow', zipCode: '90210', apiKey },
      debugLog: vi.fn(record),
      debugSuccessLog: vi.fn(record),
      debugWarnLog: vi.fn(record),
      warnLog: vi.fn(record),
      infoLog: vi.fn(record),
      errorLog: vi.fn(record),
      successLog: vi.fn(record),
      executeApiRequestWithFallback: vi.fn(async () => ({
        body: { text: async () => '[]' },
        statusCode: 200,
        headers: {},
      })),
      parseStatus: vi.fn(),
      updateHomeKitCharacteristics: vi.fn(),
      AirQualitySensor: { StatusFault: 0 },
      hap: { Characteristic: { StatusFault: { GENERAL_FAULT: 1 } } },
    }

    await AirQualitySensor.prototype.refreshStatus.call(sensor as any)

    expect(sensor.executeApiRequestWithFallback).toHaveBeenCalled()
    expect(logs.join('\n')).not.toContain(apiKey)
  })
})
