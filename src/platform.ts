/* Copyright(C) 2021-2024, donavanbecker (https://github.com/donavanbecker). All rights reserved.
 *
 * platform.ts: homebridge-air.
 */
import type { API, DynamicPlatformPlugin, HAP, Logging, PlatformAccessory } from 'homebridge'

import type { AirPlatformConfig, devicesConfig } from './settings.js'

import { readFileSync } from 'node:fs'
import process from 'node:process'

import { AirQualitySensor } from './devices/airqualitysensor.js'
import { PLATFORM_NAME, PLUGIN_NAME } from './settings.js'

/**
 * HomebridgePlatform
 * This class is the main constructor for your plugin, this is where you should
 * parse the user config and discover/register accessories with Homebridge.
 */
export class AirPlatform implements DynamicPlatformPlugin {
  public accessories: PlatformAccessory[]
  public readonly api: API
  public readonly log: Logging
  protected readonly hap: HAP
  public config!: AirPlatformConfig

  platformConfig!: AirPlatformConfig
  platformLogging!: AirPlatformConfig['logging']
  debugMode!: boolean
  version!: string

  constructor(
    log: Logging,
    config: AirPlatformConfig,
    api: API,
  ) {
    this.accessories = []
    this.api = api
    this.hap = this.api.hap
    this.log = log
    // only load if configured
    if (!config) {
      return
    }

    // Plugin options into our config variables.
    this.config = {
      platform: PLATFORM_NAME,
      devices: config.devices as devicesConfig[],
      refreshRate: config.refreshRate as number,
      logging: config.logging as string,
    }

    // Plugin options into our config variables.
    this.platformConfigOptions()
    this.platformLogs()
    this.getVersion()

    // Finish initializing the platform
    this.debugLog(`Finished initializing platform: ${config.name}`);

    // verify the config
    (async () => {
      try {
        await this.verifyConfig()
        await this.debugLog('Config OK')
      } catch (e: any) {
        await this.errorLog(`Verify Config, Error Message: ${e.message}, Submit Bugs Here: https://bit.ly/homebridge-air-bug-report`)
        this.debugErrorLog(`Verify Config, Error: ${e}`)
      }
    })()

    // When this event is fired it means Homebridge has restored all cached accessories from disk.
    // Dynamic Platform plugins should only register new accessories after this event was fired,
    // in order to ensure they weren't added to homebridge already. This event can also be used
    // to start discovery of new accessories.
    this.api.on('didFinishLaunching', async () => {
      log.debug('Executed didFinishLaunching callback')
      // run the method to discover / register your devices as accessories
      try {
        await this.discoverDevices()
      } catch (e: any) {
        await this.errorLog(`Failed to Discover Devices ${JSON.stringify(e.message)}`)
        this.debugErrorLog(`Failed to Discover, Error: ${e}`)
      }
    })
  }

  /**
   * This function is invoked when homebridge restores cached accessories from disk at startup.
   * It should be used to setup event handlers for characteristics and update respective values.
   */
  async configureAccessory(accessory: PlatformAccessory) {
    await this.infoLog(`Loading accessory from cache: ${accessory.displayName}`)

    // add the restored accessory to the accessories cache so we can track if it has already been registered
    this.accessories.push(accessory)
  }

  /**
   * Verify the config passed to the plugin is valid
   */
  async verifyConfig() {
    /**
     * Hidden Device Discovery Option
     * This will disable adding any device and will just output info.
     */
    this.config.logging = this.config.logging || 'standard'

    if (!this.config.refreshRate) {
      // default 3600 seconds (1 hour)
      this.config.refreshRate! = 3600
      await this.infoLog('Using Default Refresh Rate of 1 hour')
    }
    // Device Config
    if (this.config.devices) {
      for (const deviceConfig of this.config.devices) {
        if (!deviceConfig.apiKey) {
          await this.errorLog('Missing Your AirNow ApiKey')
        }
        if (!deviceConfig.zipCode) {
          await this.errorLog('Missing your Zip Code')
        }
      }
    } else {
      await this.errorLog('verifyConfig, No Device Config')
    }
  }

  /**
   * This method is used to discover the your location and devices.
   * Accessories are registered by either their DeviceClass, DeviceModel, or DeviceID
   */
  async discoverDevices() {
    try {
      for (const device of this.config.devices!) {
        await this.infoLog(`Discovered ${device.locationName}`)
        this.createAirQualitySensor(device)
      }
    } catch {
      await this.errorLog('discoverDevices, No Device Config')
    }
  }

  private async createAirQualitySensor(device: any) {
    const uuid = this.api.hap.uuid.generate(device.locationName + device.apiKey + device.zipCode)

    // see if an accessory with the same uuid has already been registered and restored from
    // the cached devices we stored in the `configureAccessory` method above
    const existingAccessory = this.accessories.find(accessory => accessory.UUID === uuid)

    if (existingAccessory) {
      // the accessory already exists
      if (!device.delete) {
        // if you need to update the accessory.context then you should run `api.updatePlatformAccessories`. eg.:
        existingAccessory.context.device = device
        existingAccessory.displayName = await this.validateAndCleanDisplayName(device.locationName, 'locationName', device.locationName)
        existingAccessory.context.serialNumber = device.zipCode
        existingAccessory.context.model = `Forecast by Zip Code`
        existingAccessory.context.FirmwareRevision = device.firmware ?? await this.getVersion()
        this.api.updatePlatformAccessories([existingAccessory])
        // Restore accessory
        await this.infoLog(`Restoring existing accessory from cache: ${existingAccessory.displayName}`)
        // create the accessory handler for the restored accessory
        // this is imported from `platformAccessory.ts`
        new AirQualitySensor(this, existingAccessory, device)
        await this.debugLog(`${device.locationName} uuid: ${device.locationName + device.apiKey + device.zipCode}`)
      } else {
        this.unregisterPlatformAccessories(existingAccessory)
      }
    } else if (!device.delete) {
      // create a new accessory
      const accessory = new this.api.platformAccessory(device.locationName, uuid)

      // store a copy of the device object in the `accessory.context`
      // the `context` property can be used to store any data about the accessory you may need
      accessory.context.device = device
      accessory.displayName = await this.validateAndCleanDisplayName(device.locationName, 'locationName', device.locationName)
      accessory.context.serialNumber = device.zipCode
      accessory.context.model = `Forecast by Zip Code`
      accessory.context.FirmwareRevision = device.firmware ?? await this.getVersion()
      // the accessory does not yet exist, so we need to create it
      await this.infoLog(`Adding new accessory: ${device.locationName}`)
      // create the accessory handler for the newly create accessory
      // this is imported from `platformAccessory.ts`
      new AirQualitySensor(this, accessory, device)
      await this.debugLog(`${device.locationName} uuid: ${device.locationName + device.apiKey + device.zipCode}`)

      // link the accessory to your platform
      this.api.registerPlatformAccessories(PLUGIN_NAME, PLATFORM_NAME, [accessory])
      this.accessories.push(accessory)
    } else {
      this.debugErrorLog(`Unable to Register new device: ${JSON.stringify(device.locationName)}`)
    }
  }

  public async unregisterPlatformAccessories(existingAccessory: PlatformAccessory) {
    // remove platform accessories when no longer present
    this.api.unregisterPlatformAccessories(PLUGIN_NAME, PLATFORM_NAME, [existingAccessory])
    await this.warnLog(`Removing existing accessory from cache: ${existingAccessory.displayName}`)
  }

  async platformConfigOptions() {
    const platformConfig: AirPlatformConfig['options'] = {}
    if (this.config.options) {
      if (this.config.logging) {
        platformConfig.logging = this.config.logging
      }
      if (this.config.refreshRate) {
        platformConfig.refreshRate = this.config.refreshRate
      }
      if (Object.entries(platformConfig).length !== 0) {
        this.debugLog(`Platform Config: ${JSON.stringify(platformConfig)}`)
      }
      this.platformConfig = platformConfig
    }
  }

  async platformLogs() {
    this.debugMode = process.argv.includes('-D') || process.argv.includes('--debug')
    this.platformLogging = this.config.options?.logging ?? 'standard'
    if (this.config.options?.logging === 'debug' || this.config.options?.logging === 'standard' || this.config.options?.logging === 'none') {
      this.platformLogging = this.config.options.logging
      if (await this.loggingIsDebug()) {
        this.debugWarnLog(`Using Config Logging: ${this.platformLogging}`)
      }
    } else if (this.debugMode) {
      this.platformLogging = 'debugMode'
      if (await this.loggingIsDebug()) {
        this.debugWarnLog(`Using ${this.platformLogging} Logging`)
      }
    } else {
      this.platformLogging = 'standard'
      if (await this.loggingIsDebug()) {
        this.debugWarnLog(`Using ${this.platformLogging} Logging`)
      }
    }
    if (this.debugMode) {
      this.platformLogging = 'debugMode'
    }
  }

  async getVersion() {
    const json = JSON.parse(
      readFileSync(
        new URL('../package.json', import.meta.url),
        'utf-8',
      ),
    )
    await this.debugLog(`Plugin Version: ${json.version}`)
    this.version = json.version
  }

  /**
   * Validate and clean a string value for a Name Characteristic.
   * @param displayName - The display name of the accessory.
   * @param name - The name of the characteristic.
   * @param value - The value to be validated and cleaned.
   * @returns The cleaned string value.
   */
  async validateAndCleanDisplayName(displayName: string, name: string, value: string): Promise<string> {
    if (this.config.options?.allowInvalidCharacters) {
      return value
    } else {
      const validPattern = /^[\p{L}\p{N}][\p{L}\p{N} ']*[\p{L}\p{N}]$/u
      const invalidCharsPattern = /[^\p{L}\p{N} ']/gu
      const invalidStartEndPattern = /^[^\p{L}\p{N}]+|[^\p{L}\p{N}]+$/gu

      if (typeof value === 'string' && !validPattern.test(value)) {
        this.warnLog(`WARNING: The accessory '${displayName}' has an invalid '${name}' characteristic ('${value}'). Please use only alphanumeric, space, and apostrophe characters. Ensure it starts and ends with an alphabetic or numeric character, and avoid emojis. This may prevent the accessory from being added in the Home App or cause unresponsiveness.`)

        // Remove invalid characters
        if (invalidCharsPattern.test(value)) {
          const before = value
          this.warnLog(`Removing invalid characters from '${name}' characteristic, if you feel this is incorrect,  please enable \'allowInvalidCharacter\' in the config to allow all characters`)
          value = value.replace(invalidCharsPattern, '')
          this.warnLog(`${name} Before: '${before}' After: '${value}'`)
        }

        // Ensure it starts and ends with an alphanumeric character
        if (invalidStartEndPattern.test(value)) {
          const before = value
          this.warnLog(`Removing invalid starting or ending characters from '${name}' characteristic, if you feel this is incorrect, please enable \'allowInvalidCharacter\' in the config to allow all characters`)
          value = value.replace(invalidStartEndPattern, '')
          this.warnLog(`${name} Before: '${before}' After: '${value}'`)
        }
      }

      return value
    }
  }

  /**
   * If device level logging is turned on, log to log.warn
   * Otherwise send debug logs to log.debug
   */
  async infoLog(...log: any[]): Promise<void> {
    if (await this.enablingPlatformLogging()) {
      this.log.info(String(...log))
    }
  }

  async successLog(...log: any[]): Promise<void> {
    if (await this.enablingPlatformLogging()) {
      this.log.success(String(...log))
    }
  }

  async debugSuccessLog(...log: any[]): Promise<void> {
    if (await this.enablingPlatformLogging()) {
      if (await this.loggingIsDebug()) {
        this.log.success('[DEBUG]', String(...log))
      }
    }
  }

  async warnLog(...log: any[]): Promise<void> {
    if (await this.enablingPlatformLogging()) {
      this.log.warn(String(...log))
    }
  }

  async debugWarnLog(...log: any[]): Promise<void> {
    if (await this.enablingPlatformLogging()) {
      if (await this.loggingIsDebug()) {
        this.log.warn('[DEBUG]', String(...log))
      }
    }
  }

  async errorLog(...log: any[]): Promise<void> {
    if (await this.enablingPlatformLogging()) {
      this.log.error(String(...log))
    }
  }

  async debugErrorLog(...log: any[]): Promise<void> {
    if (await this.enablingPlatformLogging()) {
      if (await this.loggingIsDebug()) {
        this.log.error('[DEBUG]', String(...log))
      }
    }
  }

  async debugLog(...log: any[]): Promise<void> {
    if (await this.enablingPlatformLogging()) {
      if (this.platformLogging === 'debug') {
        this.log.info('[DEBUG]', String(...log))
      } else if (this.platformLogging === 'debugMode') {
        this.log.debug(String(...log))
      }
    }
  }

  async loggingIsDebug(): Promise<boolean> {
    return this.platformLogging === 'debugMode' || this.platformLogging === 'debug'
  }

  async enablingPlatformLogging(): Promise<boolean> {
    return this.platformLogging === 'debugMode' || this.platformLogging === 'debug' || this.platformLogging === 'standard'
  }
}
