/* Copyright(C) 2021-2024, donavanbecker (https://github.com/donavanbecker). All rights reserved.
 *
 * airqualitysensor.ts: @homebridge-plugins/homebridge-air.
 */
import type { CharacteristicValue, PlatformAccessory, Service } from 'homebridge'
import type { Subscription } from 'rxjs'

import type { AirPlatform } from '../platform.js'
import type { AirNowAirQualityDataArray, AqicnData, devicesConfig, Pollutant } from '../settings.js'

import { interval } from 'rxjs'
import striptags from 'striptags'
import { Agent, request } from 'undici'

import {
  AirNowUrl,
  AqicnUrl,
  aqiToConcentration,
  getAqicnError,
  hasCoordinates,
  HomeKitAQI,
  normaliseAirNowRecords,
  normaliseAqicnAqi,
  REQUEST_RATE_LIMIT_CONFIG,
  REQUEST_TIMEOUT_CONFIG,
  resolveAqicnLocationSegment,
  resolveConfigDeviceName,
  resolveProviderStationName,
} from '../settings.js'
import { airNowEmptyResultMessages, safeTimerMs } from '../utils.js'
import { deviceBase } from './device.js'

const defaultApiAgent = new Agent({
  connect: {
    timeout: REQUEST_TIMEOUT_CONFIG.DEFAULT_TIMEOUT,
    autoSelectFamily: true,
    autoSelectFamilyAttemptTimeout: REQUEST_TIMEOUT_CONFIG.AUTO_SELECT_FAMILY_ATTEMPT_TIMEOUT,
  },
})

const noFamilyAutoSelectAgent = new Agent({
  connect: {
    timeout: REQUEST_TIMEOUT_CONFIG.DEFAULT_TIMEOUT,
    autoSelectFamily: false,
  },
})

/**
 * Platform Accessory
 * An instance of this class is created for each accessory your platform registers
 * Each accessory may expose multiple services of different service types.
 */
export class AirQualitySensor extends deviceBase {
  // Service
  public AirQualitySensor!: {
    Service: Service
    Name: CharacteristicValue
    AirQuality: CharacteristicValue
    OzoneDensity: CharacteristicValue
    NitrogenDioxideDensity: CharacteristicValue
    SulphurDioxideDensity: CharacteristicValue
    PM2_5Density: CharacteristicValue
    PM10Density: CharacteristicValue
    CarbonMonoxideLevel: CharacteristicValue
    StatusFault: CharacteristicValue
  }

  // Track which pollutants have valid data to prevent unnecessary characteristic updates
  private availablePollutants: Set<string> = new Set()

  // Updates
  SensorUpdateInProgress!: boolean
  private updateSubscription?: Subscription
  deviceStatus: any

  // Caching to follow AirNow best practices - observations update hourly
  // Cache for 10 minutes minimum (AirNow updates between 10-30 min past the hour)
  private lastRequestTime: number = 0
  private lastResponseData: AirNowAirQualityDataArray | AqicnData['data'] | null = null
  private readonly cacheMaxAge: number = REQUEST_RATE_LIMIT_CONFIG.CACHE_MAX_AGE
  private apiCallCount: number = 0
  private apiCallResetTime: number = Date.now() + REQUEST_RATE_LIMIT_CONFIG.CALL_WINDOW_MS

  constructor(
    readonly platform: AirPlatform,
    accessory: PlatformAccessory,
    device: devicesConfig,
  ) {
    super(platform, accessory, device)

    // AirQuality Sensor Service
    this.debugLog('Configure AirQuality Sensor Service')
    accessory.context.AirQualitySensor = accessory.context.AirQualitySensor ?? {}
    this.AirQualitySensor = {
      Name: this.accessory.displayName,
      Service: this.accessory.getService(this.hap.Service.AirQualitySensor) ?? this.accessory.addService(this.hap.Service.AirQualitySensor),
      AirQuality: accessory.context.AirQuality ?? this.hap.Characteristic.AirQuality.EXCELLENT,
      StatusFault: accessory.context.StatusFault ?? this.hap.Characteristic.StatusFault.NO_FAULT,
      OzoneDensity: accessory.context.OzoneDensity ?? 0,
      NitrogenDioxideDensity: accessory.context.NitrogenDioxideDensity ?? 0,
      SulphurDioxideDensity: accessory.context.SulphurDioxideDensity ?? 0,
      PM2_5Density: accessory.context.PM2_5Density ?? 0,
      PM10Density: accessory.context.PM10Density ?? 0,
      CarbonMonoxideLevel: accessory.context.CarbonMonoxideLevel ?? 0,
    }
    accessory.context.AirQualitySensor = this.AirQualitySensor as object

    // Add AirQuality Sensor Service's Characteristics
    this.AirQualitySensor.Service.setCharacteristic(this.hap.Characteristic.Name, this.AirQualitySensor.Name)

    // this is subject we use to track when we need to POST changes to the Air API
    this.SensorUpdateInProgress = false

    // Retrieve initial values and updateHomekit
    this.refreshStatus()

    // Start an update interval. The overlap guard is checked inside refreshStatus
    // now: it used to be a `skipWhile`, which stops testing its predicate for good
    // after the first false, and nothing ever raised the flag anyway - so a stalled
    // request could be joined by a second one on the next tick, both writing to the
    // same fields and each counting against the provider's rate limit.
    this.updateSubscription = interval(safeTimerMs(this.deviceRefreshRate * 1000))
      .subscribe(async () => {
        await this.refreshStatus()
      })
  }

  /**
   * Parse the device status from the Air api
   */
  async parseStatus() {
    try {
      const provider = this.device.provider
      const status = provider === 'airnow' ? this.deviceStatus[0] : this.deviceStatus

      // Clear previous pollutant availability tracking at the start
      this.availablePollutants.clear()

      if (provider === 'airnow' && !status) {
        this.errorLog('AirNow air quality Configuration Error - Invalid ZipCode for %s.', provider)
        this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.GENERAL_FAULT
      } else if (provider === 'airnow' && typeof status.AQI === 'undefined') {
        this.errorLog('AirNow air quality Observation Error - %s for %s.', striptags(JSON.stringify(this.deviceStatus)), provider)
        this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.GENERAL_FAULT
      } else if (provider === 'aqicn' && (!this.deviceStatus || typeof this.deviceStatus.aqi === 'undefined')) {
        this.errorLog('AQICN air quality Data Error - Invalid response structure or missing AQI data for %s.', provider)
        await this.debugLog('AQICN response structure: %s', JSON.stringify(this.deviceStatus))
        this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.GENERAL_FAULT
      } else if (provider === 'airnow' || provider === 'aqicn') {
        // Set the main AirQuality using the overall AQI value
        if (provider === 'aqicn') {
          // For AQICN, use the main aqi value for overall air quality
          const mainAqi = this.deviceStatus.aqi
          if (typeof mainAqi === 'number' && !Number.isNaN(mainAqi)) {
            this.AirQualitySensor.AirQuality = HomeKitAQI(Math.max(0, mainAqi))
            await this.debugLog(`${provider} main AQI: ${mainAqi} -> HomeKit category: ${this.AirQualitySensor.AirQuality}`)
          }
        }

        // Process individual pollutants for their specific density characteristics
        const pollutants = provider === 'airnow' ? ['O3', 'PM2.5', 'PM10'] : ['o3', 'no2', 'so2', 'pm25', 'pm10', 'co']
        let pollutantCount = 0

        // AirNow publishes an index per pollutant and defines the overall AQI as
        // the worst of them. This used to be assigned inside the loop, so the
        // last pollutant processed won instead - during an ozone alert HomeKit
        // could read 'Excellent' because PM10 happened to be reported last, and
        // no air quality automation would ever fire.
        let airNowWorstAqi = -1

        for (const pollutant of pollutants) {
          const param = provider === 'airnow' ? this.deviceStatus.find((p: { ParameterName: string }) => p.ParameterName === pollutant) : this.deviceStatus.iaqi[pollutant]?.v
          if (param !== undefined) {
            const aqi = provider === 'airnow' ? Number.parseFloat(param.AQI.toString()) : Number.parseFloat(param.toString())
            if (!Number.isNaN(aqi)) {
              pollutantCount++

              // Both providers give us an AQI sub-index, never a concentration,
              // so convert before writing to a density characteristic (#77).
              const key = pollutant.toLowerCase() === 'pm2.5' ? 'pm25' : pollutant.toLowerCase() as Pollutant
              const concentration = aqiToConcentration(key, aqi, provider)
              await this.debugLog(`${provider} ${pollutant} AQI: ${aqi} -> ${concentration ?? 'no concentration'}`)

              if (concentration !== undefined) {
                switch (key) {
                  case 'o3':
                    this.AirQualitySensor.OzoneDensity = concentration
                    this.availablePollutants.add('OzoneDensity')
                    break
                  case 'pm25':
                    this.AirQualitySensor.PM2_5Density = concentration
                    this.availablePollutants.add('PM2_5Density')
                    break
                  case 'pm10':
                    this.AirQualitySensor.PM10Density = concentration
                    this.availablePollutants.add('PM10Density')
                    break
                  case 'no2':
                    this.AirQualitySensor.NitrogenDioxideDensity = concentration
                    this.availablePollutants.add('NitrogenDioxideDensity')
                    break
                  case 'so2':
                    this.AirQualitySensor.SulphurDioxideDensity = concentration
                    this.availablePollutants.add('SulphurDioxideDensity')
                    break
                  case 'co':
                    // Recorded for the log only. CarbonMonoxideLevel is not a
                    // characteristic of the Air Quality Sensor service, so adding
                    // it produces a Homebridge characteristic warning and the
                    // Home app never shows the value - it only reads carbon
                    // monoxide from a Carbon Monoxide Sensor service.
                    this.AirQualitySensor.CarbonMonoxideLevel = concentration
                    break
                }
              } else {
                await this.debugWarnLog(`${provider} ${pollutant} AQI ${aqi} is outside the EPA breakpoints, leaving the reading unchanged`)
              }
              // For AirNow, the overall reading is the worst of the pollutants
              if (provider === 'airnow') {
                airNowWorstAqi = Math.max(airNowWorstAqi, aqi)
              }
            }
          } else {
            await this.debugLog(`${provider} ${pollutant} data not available`)
          }
        }

        if (provider === 'airnow' && airNowWorstAqi >= 0) {
          this.AirQualitySensor.AirQuality = HomeKitAQI(airNowWorstAqi)
          await this.debugLog(`${provider} worst pollutant AQI: ${airNowWorstAqi} -> HomeKit category: ${this.AirQualitySensor.AirQuality}`)
        }

        if (pollutantCount === 0) {
          this.warnLog(`${provider} No pollutant data found in response. Available iaqi keys: ${provider === 'aqicn' ? JSON.stringify(Object.keys(this.deviceStatus.iaqi || {})) : 'N/A'}`)
        } else {
          this.infoLog(`${provider} air quality AQI is: ${this.AirQualitySensor.AirQuality} (${pollutantCount} pollutants found)`)
        }
        this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.NO_FAULT
        await this.applyProviderStationName()
      } else {
        await this.errorLog('Unknown air quality provider: %s.', provider)
      }
    } catch (e: any) {
      await this.errorLog(`failed to parseStatus, Error Message: ${JSON.stringify(e.message ?? e)}`)
      await this.apiError(e)
    }
  }

  /**
   * Name a newly added accessory after the station its data describes, e.g.
   * 'Kirchackerstrasse' rather than 'Station 92323' (#69).
   *
   * This only ever runs for accessories the plugin has just created. Anything
   * already in HomeKit keeps its current name, because that name was the
   * user's decision. If the provider gives us no name we leave the flag set so
   * the next refresh can try again, and the same goes for a device the config
   * names itself, so that name can be cleared again later.
   */
  async applyProviderStationName(): Promise<void> {
    if (!this.accessory.context.nameFromProvider) {
      return
    }

    // A name in the config outranks the station's own, so leave the accessory
    // as the platform named it. The flag stays armed on purpose: clearing that
    // name later has to be able to hand naming back to the provider, which it
    // cannot do once this has been disarmed
    if (resolveConfigDeviceName(this.device)) {
      return
    }

    const stationName = resolveProviderStationName(this.device.provider, this.deviceStatus)
    if (!stationName) {
      return
    }

    const cleanName = await this.platform.validateAndCleanDisplayName(stationName, 'station name', stationName)
    this.accessory.context.nameFromProvider = false
    if (!cleanName || cleanName === this.accessory.displayName) {
      return
    }

    await this.infoLog(`Naming accessory after its station: '${this.accessory.displayName}' -> '${cleanName}'`)
    this.accessory.context.providerName = cleanName
    this.accessory.updateDisplayName(cleanName)
    this.AirQualitySensor.Name = cleanName
    this.AirQualitySensor.Service.updateCharacteristic(this.hap.Characteristic.Name, cleanName)
    this.accessory
      .getService(this.hap.Service.AccessoryInformation)
      ?.updateCharacteristic(this.hap.Characteristic.Name, cleanName)
      .updateCharacteristic(this.hap.Characteristic.ConfiguredName, cleanName)
    this.api.updatePlatformAccessories([this.accessory])
  }

  /**
   * Reverse geocode lat/long to get zip code using Nominatim (OpenStreetMap)
   * This is used as a fallback when lat/long endpoint fails
   */
  async reverseGeocodeToZipCode(latitude: number, longitude: number): Promise<{ zipCode: string, city: string } | null> {
    try {
      await this.debugLog(`Attempting reverse geocoding for coordinates: ${latitude}, ${longitude}`)
      const geocodeUrl = `https://nominatim.openstreetmap.org/reverse?format=json&lat=${latitude}&lon=${longitude}&zoom=18&addressdetails=1`

      const { body, statusCode } = await request(geocodeUrl, {
        headers: {
          'User-Agent': 'homebridge-air/1.0',
        },
        headersTimeout: REQUEST_TIMEOUT_CONFIG.GEOCODE_TIMEOUT,
        bodyTimeout: REQUEST_TIMEOUT_CONFIG.GEOCODE_TIMEOUT,
        dispatcher: defaultApiAgent,
      })

      if (statusCode === 200) {
        const responseText = await body.text()
        const data = JSON.parse(responseText)

        if (data.address) {
          const zipCode = data.address.postcode
          const city = data.address.city || data.address.town || data.address.village || data.address.county

          if (zipCode && city) {
            await this.infoLog(`Reverse geocoding successful: ${city}, ZIP ${zipCode}`)
            return { zipCode, city }
          }
        }
      }

      await this.debugLog('Reverse geocoding failed or incomplete data received')
      return null
    } catch (error: any) {
      await this.debugLog(`Reverse geocoding error: ${error.message}`)
      return null
    }
  }

  /**
   * Asks the Air API for the latest device information
   */
  async refreshStatus() {
    if (this.SensorUpdateInProgress) {
      await this.debugLog('Skipping this refresh, the previous one has not finished')
      return
    }
    this.SensorUpdateInProgress = true
    try {
      // Check cache first to reduce API calls and follow AirNow best practices
      const currentTime = Date.now()
      if (this.lastResponseData && (currentTime - this.lastRequestTime) < this.cacheMaxAge) {
        const cacheAge = Math.round((currentTime - this.lastRequestTime) / 1000)
        await this.debugLog(`Using cached response (${cacheAge}s old, cache max: ${this.cacheMaxAge / 1000}s)`)
        this.deviceStatus = this.lastResponseData
        await this.parseStatus()
        await this.updateHomeKitCharacteristics()
        return
      }

      // Reset API call counter every hour (rate limiting per AirNow guidelines)
      if (currentTime > this.apiCallResetTime) {
        await this.debugLog(`Resetting API call counter (made ${this.apiCallCount} calls last hour)`)
        this.apiCallCount = 0
        this.apiCallResetTime = currentTime + REQUEST_RATE_LIMIT_CONFIG.CALL_WINDOW_MS
      }

      // Check rate limit (conservative limit to avoid issues)
      // AirNow recommends caching and limiting calls since observations update hourly
      const maxCallsPerHour = REQUEST_RATE_LIMIT_CONFIG.MAX_CALLS_PER_WINDOW
      if (this.apiCallCount >= maxCallsPerHour) {
        const timeUntilReset = Math.round((this.apiCallResetTime - currentTime) / 60000)
        await this.warnLog(`API rate limit reached (${this.apiCallCount} calls). Using cached data. Resets in ${timeUntilReset} min`)
        if (this.lastResponseData) {
          this.deviceStatus = this.lastResponseData
          await this.parseStatus()
          await this.updateHomeKitCharacteristics()
        }
        return
      }

      // Increment API call counter
      this.apiCallCount++
      await this.debugLog(`API call ${this.apiCallCount}/${maxCallsPerHour} this hour`)

      // Use correct AirNow API endpoint paths from official docs
      // https://docs.airnowapi.org/CurrentObservationsByZip/docs
      // https://docs.airnowapi.org/CurrentObservationsByLatLon/docs
      // Support flexible AQICN URL patterns: geo coordinates, city names, and full URL paths
      const AqicnCurrentObservationBy = resolveAqicnLocationSegment(this.device)
      const AirNowCurrentObservationByValue = hasCoordinates(this.device) ? `latitude=${this.device.latitude}&longitude=${this.device.longitude}` : `zipCode=${this.device.zipCode}`
      // Use correct format as per official AirNow API docs
      const providerUrls = {
        // AirNow's newer endpoint. It finds the closest reading for each pollutant
        // separately rather than needing them all inside one radius, which is the only
        // way somewhere remote gets a reading at all - 93546 came back empty from the
        // older endpoint even at 150 miles (#84). It takes zip or lat/long, and ignores
        // `distance` entirely: its own boundary is fixed at 50 miles.
        airnow: `${AirNowUrl}current/ziplatlong/?format=application/json&${AirNowCurrentObservationByValue}&API_KEY=${this.device.apiKey}`,
        aqicn: `${AqicnUrl}${AqicnCurrentObservationBy}${AqicnCurrentObservationBy ? '/' : ''}?token=${this.device.apiKey}`,
      }
      const url = providerUrls[this.device.provider]
      // Do not log `url`: it includes API_KEY / token. The Matter poller
      // never logs the request URL for the same reason.
      if (url) {
        const { body, statusCode, headers } = await this.executeApiRequestWithFallback(url)

        let response: any
        try {
          const responseText = await body.text()
          await this.debugLog(`Raw response (length: ${responseText.length}): ${responseText}`)
          await this.debugWarnLog(`statusCode: ${JSON.stringify(statusCode)}`)

          // Check for redirects (3xx status codes) - try fallback to zip code lookup
          if (statusCode >= 300 && statusCode < 400) {
            const location = headers.location
            await this.warnLog(`API returned redirect (${statusCode}). Location: ${location || 'not provided'}`)

            // If using lat/lon with AirNow, try reverse geocoding to get zip code as fallback
            if (this.device.provider === 'airnow' && hasCoordinates(this.device)) {
              await this.infoLog('Attempting reverse geocoding to find zip code as fallback...')
              const geoData = await this.reverseGeocodeToZipCode(this.device.latitude, this.device.longitude)

              if (geoData?.zipCode) {
                await this.infoLog(`Found zip code ${geoData.zipCode} for ${geoData.city}. Retrying with zip code...`)
                // Temporarily update device config to use zip code
                const originalZipCode = this.device.zipCode
                this.device.zipCode = geoData.zipCode
                this.device.city = geoData.city

                // Build new URL with zip code
                const fallbackUrl = `${AirNowUrl}current/ziplatlong/?format=application/json&zipCode=${geoData.zipCode}&API_KEY=${this.device.apiKey}`

                try {
                  const fallbackResponse = await this.executeApiRequestWithFallback(fallbackUrl)

                  const fallbackText = await fallbackResponse.body.text()
                  if (fallbackResponse.statusCode === 200 && fallbackText && fallbackText.trim().length > 0) {
                    response = JSON.parse(fallbackText)
                    await this.successLog(`Fallback to zip code successful! Using ${geoData.city}, ${geoData.zipCode}`)
                    // Process the successful response
                    this.deviceStatus = response
                    this.lastResponseData = response
                    this.lastRequestTime = Date.now()
                    await this.parseStatus()
                    await this.updateHomeKitCharacteristics()
                    return
                  } else {
                    await this.warnLog(`Fallback zip code lookup also failed (Status: ${fallbackResponse.statusCode})`)
                  }
                } catch (fallbackError: any) {
                  await this.debugLog(`Fallback zip code request failed: ${fallbackError.message}`)
                } finally {
                  // Restore original zip code if we had one
                  if (originalZipCode) {
                    this.device.zipCode = originalZipCode
                  }
                }
              } else {
                await this.warnLog('Could not determine zip code from coordinates')
              }
            }

            await this.errorLog('The AirNow API endpoint may have changed or requires different parameters.')
            await this.debugLog('Try using zipCode in your config, or check if your API key is valid.')
            this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.GENERAL_FAULT
            return
          }

          if (!responseText || responseText.trim().length === 0) {
            // Try reverse geocoding fallback for empty responses too
            if (this.device.provider === 'airnow' && hasCoordinates(this.device) && !this.device.zipCode) {
              await this.infoLog('Empty response - attempting reverse geocoding fallback...')
              const geoData = await this.reverseGeocodeToZipCode(this.device.latitude, this.device.longitude)

              if (geoData?.zipCode) {
                await this.infoLog(`Found zip code ${geoData.zipCode}. Retrying with zip code...`)
                this.device.zipCode = geoData.zipCode
                this.device.city = geoData.city

                const fallbackUrl = `${AirNowUrl}current/ziplatlong/?format=application/json&zipCode=${geoData.zipCode}&API_KEY=${this.device.apiKey}`

                try {
                  const fallbackResponse = await this.executeApiRequestWithFallback(fallbackUrl)

                  const fallbackText = await fallbackResponse.body.text()
                  if (fallbackResponse.statusCode === 200 && fallbackText && fallbackText.trim().length > 0) {
                    response = JSON.parse(fallbackText)
                    await this.successLog(`Fallback to zip code successful! Will use ${geoData.city}, ${geoData.zipCode} going forward`)
                    this.deviceStatus = response
                    this.lastResponseData = response
                    this.lastRequestTime = Date.now()
                    await this.parseStatus()
                    await this.updateHomeKitCharacteristics()
                    return
                  }
                } catch (fallbackError: any) {
                  await this.debugLog(`Fallback zip code request failed: ${fallbackError.message}`)
                }
              }
            }

            await this.errorLog(`Empty response body received from ${this.device.provider} API (Status: ${statusCode})`)
            await this.errorLog('This usually means no air quality data is available for your location.')
            await this.errorLog('Verify your zip code or coordinates are correct - AirNow looks up to 50 miles for each pollutant.')
            await this.debugLog(`Current settings - Lat: ${this.device.latitude}, Lon: ${this.device.longitude}, Zip: ${this.device.zipCode}`)
            this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.GENERAL_FAULT
            return
          }

          response = JSON.parse(responseText)
        } catch (parseError: any) {
          await this.errorLog(`Failed to parse JSON response from ${this.device.provider} API: ${parseError.message}`)
          await this.debugLog(`Parse error details: ${JSON.stringify({ code: parseError.code, name: parseError.name })}`)
          this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.GENERAL_FAULT
          return
        }

        await this.debugLog(`response: ${JSON.stringify(response)}`)

        if (statusCode !== 200) {
          const errorMessage = `${this.device.provider === 'airnow' ? 'AirNow' : 'World Air Quality Index'} API returned status ${statusCode}`
          await this.errorLog(`${errorMessage} for provider %s.`, this.device.provider)
          this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.GENERAL_FAULT
          await this.debugLog(`Error response: ${JSON.stringify(response)}`)
          await this.apiError(response)
        } else {
          // Validate response structure before processing
          if (!response) {
            await this.errorLog(`Empty response received from ${this.device.provider} API`)
            this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.GENERAL_FAULT
            return
          }

          if (this.device.provider === 'aqicn') {
            const aqicnResponse = response as AqicnData
            // Surface the real API reason, including errors AQICN nests inside
            // data while still reporting an 'ok' top-level status (#7)
            const aqicnError = getAqicnError(aqicnResponse)
            if (aqicnError) {
              await this.errorLog(`AQICN API Error - ${aqicnError}`)
              await this.debugLog(`AQICN response structure: ${JSON.stringify(aqicnResponse)}`)
              this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.GENERAL_FAULT
              await this.apiError(aqicnResponse)
              return
            }
            // The overall aqi can be a numeric string, '-' or missing on
            // community stations; normalise it (falling back to the highest
            // pollutant sub-index) before validating (#7)
            const normalisedAqi = normaliseAqicnAqi(aqicnResponse.data)
            if (normalisedAqi === undefined) {
              await this.errorLog('AQICN API Error - Missing AQI data in response')
              await this.debugLog(`AQICN response structure: ${JSON.stringify(aqicnResponse.data)}`)
              this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.GENERAL_FAULT
              return
            }
            aqicnResponse.data.aqi = normalisedAqi
            this.deviceStatus = aqicnResponse.data
            // Cache the successful response (stations publish roughly hourly)
            this.lastResponseData = aqicnResponse.data
            this.lastRequestTime = Date.now()
            await this.debugLog(`Data cached. Will reuse for ${this.cacheMaxAge / 1000}s (AQICN updates hourly)`)
          } else {
            // Validate AirNow response structure
            // AirNow's newer endpoint answers with a different shape to the older one,
            // and a location can be served by one and not the other while they migrate.
            // Normalise both onto the shape the rest of this file reads (#84).
            const airnowResponse = normaliseAirNowRecords(response) as AirNowAirQualityDataArray
            if (!Array.isArray(airnowResponse) || airnowResponse.length === 0) {
              for (const message of airNowEmptyResultMessages(airnowResponse)) {
                await this.errorLog(message)
              }
              await this.debugLog(`AirNow response structure: ${JSON.stringify(airnowResponse)}`)
              this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.GENERAL_FAULT
              return
            }
            this.deviceStatus = airnowResponse
            // Cache the successful response (following AirNow best practices for hourly updates)
            this.lastResponseData = airnowResponse
            this.lastRequestTime = Date.now()
            await this.debugLog(`Data cached. Will reuse for ${this.cacheMaxAge / 1000}s (AirNow updates hourly)`)
            this.lastRequestTime = Date.now()
          }
          await this.parseStatus()
        }
      } else {
        await this.errorLog('Unknown air quality provider: %s.', this.device.provider)
      }
      await this.updateHomeKitCharacteristics()
    } catch (e: any) {
      // Improve error message handling for different error types
      const errorMessage = e?.message || e?.code || e?.name || 'Unknown error'

      // Handle specific error types for better debugging
      if (e?.code === 'UND_ERR_CONNECT_TIMEOUT' || e?.code === 'ETIMEDOUT') {
        await this.errorLog(`API request timeout for ${this.device.provider} - check network connectivity`)
        this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.GENERAL_FAULT
      } else if (e?.code === 'ENOTFOUND' || e?.code === 'ECONNREFUSED') {
        await this.errorLog(`Network error for ${this.device.provider} API - ${errorMessage}`)
        this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.GENERAL_FAULT
      } else {
        await this.errorLog(`Failed to update status for ${this.device.provider}, Error: ${errorMessage}`)
        this.AirQualitySensor.StatusFault = this.hap.Characteristic.StatusFault.GENERAL_FAULT
      }

      // Log additional context for debugging (limit to avoid performance issues)
      const limitedError = {
        message: e?.message,
        code: e?.code,
        name: e?.name,
      }
      await this.debugLog(`Error object: ${JSON.stringify(limitedError)}`)
      await this.debugLog(`Provider: ${this.device.provider}, City: ${this.device.city || 'N/A'}`)

      await this.apiError(e)
    } finally {
      this.SensorUpdateInProgress = false
    }
  }

  /**
   * Stop polling. Without this the interval keeps the process alive and keeps
   * calling the provider after Homebridge has asked the plugin to stop.
   */
  public shutdown(): void {
    this.updateSubscription?.unsubscribe()
    this.updateSubscription = undefined
  }

  private isTimeoutError(error: any): boolean {
    const directCode = error?.code
    const directName = error?.name
    const nestedTimeout = Array.isArray(error?.errors)
      && error.errors.some((nested: any) => nested?.code === 'ETIMEDOUT' || nested?.code === 'UND_ERR_CONNECT_TIMEOUT')

    return directCode === 'ETIMEDOUT'
      || directCode === 'UND_ERR_CONNECT_TIMEOUT'
      || directName === 'AggregateError'
      || nestedTimeout
  }

  private async executeApiRequestWithFallback(url: string) {
    const requestOptions = {
      headersTimeout: REQUEST_TIMEOUT_CONFIG.DEFAULT_TIMEOUT,
      bodyTimeout: REQUEST_TIMEOUT_CONFIG.DEFAULT_TIMEOUT,
      dispatcher: defaultApiAgent,
    }

    try {
      return await request(url, requestOptions)
    } catch (error: any) {
      if (!this.isTimeoutError(error)) {
        throw error
      }

      await this.debugWarnLog('Request timeout detected, retrying with network family auto-selection disabled')
      return request(url, {
        headersTimeout: REQUEST_TIMEOUT_CONFIG.DEFAULT_TIMEOUT,
        bodyTimeout: REQUEST_TIMEOUT_CONFIG.DEFAULT_TIMEOUT,
        dispatcher: noFamilyAutoSelectAgent,
      })
    }
  }

  /**
   * Updates the status for each of the HomeKit Characteristics
   */
  async updateHomeKitCharacteristics(): Promise<void> {
    // AirQuality (always available)
    await this.updateCharacteristic(this.AirQualitySensor.Service, this.hap.Characteristic.AirQuality, this.AirQualitySensor.AirQuality, 'AirQuality')

    // Only update characteristics for pollutants that have data available
    if (this.availablePollutants.has('OzoneDensity')) {
      await this.updateCharacteristic(this.AirQualitySensor.Service, this.hap.Characteristic.OzoneDensity, this.AirQualitySensor.OzoneDensity, 'OzoneDensity')
    }
    if (this.availablePollutants.has('NitrogenDioxideDensity')) {
      await this.updateCharacteristic(this.AirQualitySensor.Service, this.hap.Characteristic.NitrogenDioxideDensity, this.AirQualitySensor.NitrogenDioxideDensity, 'NitrogenDioxideDensity')
    }
    if (this.availablePollutants.has('SulphurDioxideDensity')) {
      await this.updateCharacteristic(this.AirQualitySensor.Service, this.hap.Characteristic.SulphurDioxideDensity, this.AirQualitySensor.SulphurDioxideDensity, 'SulphurDioxideDensity')
    }
    if (this.availablePollutants.has('PM2_5Density')) {
      await this.updateCharacteristic(this.AirQualitySensor.Service, this.hap.Characteristic.PM2_5Density, this.AirQualitySensor.PM2_5Density, 'PM2_5Density')
    }
    if (this.availablePollutants.has('PM10Density')) {
      await this.updateCharacteristic(this.AirQualitySensor.Service, this.hap.Characteristic.PM10Density, this.AirQualitySensor.PM10Density, 'PM10Density')
    }

    // StatusFault (always available)
    await this.updateCharacteristic(this.AirQualitySensor.Service, this.hap.Characteristic.StatusFault, this.AirQualitySensor.StatusFault, 'StatusFault')
  }

  // eslint-disable-next-line unused-imports/no-unused-vars
  public async apiError(_e: any): Promise<void> {
    // Set StatusFault to indicate an error state - don't set measurement characteristics to error objects
    this.AirQualitySensor.Service.updateCharacteristic(this.hap.Characteristic.StatusFault, this.hap.Characteristic.StatusFault.GENERAL_FAULT)
  }
}
