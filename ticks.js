import express from "express"
import axios from "axios"
import { createClient } from "redis"
import { performance } from "perf_hooks"
import fs from "fs/promises"
import path from "path"
import { fileURLToPath } from "url"

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

// Logging setup
const logFilePath = path.join(process.cwd(), "subscription_logs.txt")

async function writeLog(message) {
  const IST_OFFSET = 5.5 * 60 * 60 * 1000
  const timestamp = new Date(Date.now() + IST_OFFSET).toISOString().replace("T", " ").substring(0, 19)
  const logEntry = `[${timestamp}] ${message}\n`
  console.log(`[${timestamp}] ${message}`)
  try {
    await fs.appendFile(logFilePath, logEntry)
  } catch (error) {
    console.error("Failed to write to log file:", error)
  }
}

// Date utilities for IST
function getISTDate() {
  const IST_OFFSET = 5.5 * 60 * 60 * 1000
  const istTime = new Date(Date.now() + IST_OFFSET)
  return istTime.toISOString().split("T")[0] // Returns YYYY-MM-DD format
}

async function isFileGeneratedToday(filePath) {
  try {
    const stats = await fs.stat(filePath)
    const fileDate = new Date(stats.mtime.getTime() + 5.5 * 60 * 60 * 1000)
    const fileDateString = fileDate.toISOString().split("T")[0]
    const todayString = getISTDate()
    return fileDateString === todayString
  } catch (error) {
    return false // File doesn't exist
  }
}

await writeLog("=".repeat(80))
await writeLog("OPTION CHAIN API CLIENT - SIMPLIFIED CONTINUOUS POLLING")
await writeLog("=".repeat(80))

// Redis client with optimized settings
const redisClient = createClient({
  socket: {
    host: "172.24.169.200",
    port: 6379,
    connectTimeout: 10000,
    lazyConnect: true,
  },
})

redisClient.on("error", (err) => {
  writeLog(`Redis Client Error: ${err.message}`)
})

redisClient.on("connect", () => {
  writeLog("Redis connected successfully")
})

async function connectRedis() {
  try {
    console.time("Redis Connection")
    await redisClient.connect()
    console.timeEnd("Redis Connection")
    await writeLog("Redis client connected with optimized settings")
  } catch (error) {
    await writeLog(`Redis connection failed: ${error.message}`)
  }
}

connectRedis()

// Performance tracking
const timingSummary = {
  packets: [],
  totalPacketsStored: 0,
  totalProcessingTime: 0,
  totalStorageSizeBytes: 0,
}

const configDir = "./configs"
const MIXED_CONFIG_FILE = "mixed_config.txt"
const POLLING_INTERVAL = 60000 // 60 seconds (1 minute)
const SUBSCRIBE_CHUNK_SIZE = 5000

// File paths for local storage
const NSE_FO_FILE = "NSE_FO.json"
const NSE_EQ_FILE = "NSE_EQ.json"
const UNDERLYING_TOKEN_FILE = "underlyingtoken.json"
const STRIKE_GAPS_FILE = "strikegaps.json"
// Add the underlying config file constant
const UNDERLYING_CONFIG_FILE = "underlyingconfig.txt"

// Polling control
let pollingInterval = null

// API configuration
const API_URL = "https://activity.stoxbox.in:9016/stxauth/authentication"
const AUTH_URL = "http://activity.stoxbox.in:9000/api/authorizereq"
const AUTH_CREDENTIALS = {
  username: "bpdashboard",
  password: "YTMG3-N6DKC-DKB77-7M9GH-8HVX7",
}

let API_AUTH_TOKEN = null

// Initialize Bearer token
async function initializeAuthToken() {
  try {
    console.time("Authentication")
    await writeLog("Initializing Bearer token...")
    const authResponse = await axios.post(AUTH_URL, AUTH_CREDENTIALS, {
      headers: {
        "Content-Type": "application/json",
      },
      timeout: 10000,
    })

    if (authResponse.status !== 200) {
      throw new Error(`Auth failed: ${authResponse.status}`)
    }

    const token = authResponse.data.id_token || authResponse.data.token
    if (!token) {
      throw new Error("Token not found in response")
    }

    API_AUTH_TOKEN = token
    console.timeEnd("Authentication")
    await writeLog(`Bearer token initialized successfully`)
    return true
  } catch (error) {
    await writeLog(`Failed to initialize Bearer token: ${error.message}`)
    return false
  }
}

// Download and save NSE data locally
async function downloadAndSaveNSEData() {
  const nsefoPath = path.join(process.cwd(), NSE_FO_FILE)
  const nseeqPath = path.join(process.cwd(), NSE_EQ_FILE)

  // Check if files are already generated today
  const foGeneratedToday = await isFileGeneratedToday(nsefoPath)
  const eqGeneratedToday = await isFileGeneratedToday(nseeqPath)

  let foData = null
  let eqData = null

  // Download NSE_FO if not generated today
  if (!foGeneratedToday) {
    console.time("NSE_FO Data Download")
    await writeLog("Downloading fresh NSE_FO.json from server...")

    try {
      const response = await axios.get("https://odinscripmaster.s3.ap-south-1.amazonaws.com/scripfiles/v2/NSE_FO.json")
      foData = response.data

      // Save to local file
      await fs.writeFile(nsefoPath, JSON.stringify(foData, null, 2))
      console.timeEnd("NSE_FO Data Download")
      await writeLog(`Successfully downloaded and saved NSE_FO.json to ${nsefoPath}`)
    } catch (error) {
      await writeLog(`Failed to download NSE_FO.json: ${error.message}`)
      throw error
    }
  } else {
    await writeLog("NSE_FO.json already generated today, loading from local file...")
    try {
      const fileContent = await fs.readFile(nsefoPath, "utf8")
      foData = JSON.parse(fileContent)
      await writeLog("Successfully loaded NSE_FO.json from local file")
    } catch (error) {
      await writeLog(`Failed to load local NSE_FO.json: ${error.message}`)
      throw error
    }
  }

  // Download NSE_EQ if not generated today
  if (!eqGeneratedToday) {
    console.time("NSE_EQ Data Download")
    await writeLog("Downloading fresh NSE_EQ.json from server...")

    try {
      const response = await axios.get("https://odinscripmaster.s3.ap-south-1.amazonaws.com/scripfiles/v2/NSE_EQ.json")
      eqData = response.data

      // Save to local file
      await fs.writeFile(nseeqPath, JSON.stringify(eqData, null, 2))
      console.timeEnd("NSE_EQ Data Download")
      await writeLog(`Successfully downloaded and saved NSE_EQ.json to ${nseeqPath}`)
    } catch (error) {
      await writeLog(`Failed to download NSE_EQ.json: ${error.message}`)
      throw error
    }
  } else {
    await writeLog("NSE_EQ.json already generated today, loading from local file...")
    try {
      const fileContent = await fs.readFile(nseeqPath, "utf8")
      eqData = JSON.parse(fileContent)
      await writeLog("Successfully loaded NSE_EQ.json from local file")
    } catch (error) {
      await writeLog(`Failed to load local NSE_EQ.json: ${error.message}`)
      throw error
    }
  }

  return { foData, eqData }
}

// Generate underlying token mapping
async function generateUnderlyingTokens(eqData, foData) {
  const underlyingTokenPath = path.join(process.cwd(), UNDERLYING_TOKEN_FILE)

  // Check if file is already generated today
  if (await isFileGeneratedToday(underlyingTokenPath)) {
    await writeLog("underlyingtoken.json already generated today, skipping...")
    return
  }

  await writeLog("Generating underlyingtoken.json...")

  try {
    const eqHeaders = eqData[0]
    const eqEntries = eqData.slice(1)
    const foHeaders = foData[0]
    const foEntries = foData.slice(1)

    // Get necessary indices
    const idxSymbolEq = eqHeaders.indexOf("sSymbol")
    const idxMarketSegment = eqHeaders.indexOf("nMarketSegmentId")
    const idxToken = eqHeaders.indexOf("nToken")
    const idxSymbolFo = foHeaders.indexOf("sSymbol")

    if (idxSymbolEq === -1 || idxMarketSegment === -1 || idxToken === -1 || idxSymbolFo === -1) {
      throw new Error("Required columns not found in data")
    }

    // Build FO symbol set
    const foSymbols = new Set(foEntries.map((entry) => entry[idxSymbolFo].trim()))

    // Build dictionary of sSymbol -> combinedtoken
    const symbolTokenMap = {}
    const combinedTokens = []

    for (const entry of eqEntries) {
      const sSymbol = entry[idxSymbolEq].trim()
      if (foSymbols.has(sSymbol)) {
        const combinedToken = `${entry[idxMarketSegment]}_${entry[idxToken]}`
        symbolTokenMap[sSymbol] = combinedToken
        combinedTokens.push(combinedToken)
      }
    }

    // Save dictionary to JSON
    await fs.writeFile(underlyingTokenPath, JSON.stringify(symbolTokenMap, null, 2))

    // Save combined tokens to TXT
    const combinedTokensPath = path.join(process.cwd(), "combinedtokens.txt")
    await fs.writeFile(combinedTokensPath, combinedTokens.join(","))

    await writeLog(`Generated underlyingtoken.json with ${Object.keys(symbolTokenMap).length} symbols`)
    await writeLog(`Generated combinedtokens.txt with ${combinedTokens.length} tokens`)
  } catch (error) {
    await writeLog(`Error generating underlying tokens: ${error.message}`)
    throw error
  }
}

// Generate strike gaps mapping
async function generateStrikeGaps(foData) {
  const strikeGapsPath = path.join(process.cwd(), STRIKE_GAPS_FILE)

  // Check if file is already generated today
  if (await isFileGeneratedToday(strikeGapsPath)) {
    await writeLog("strikegaps.json already generated today, skipping...")
    return
  }

  await writeLog("Generating strikegaps.json...")

  try {
    const headers = foData[0]
    const entries = foData.slice(1)

    const idxSymbol = headers.indexOf("sSymbol")
    const idxStrike = headers.indexOf("nStrikePrice")

    if (idxSymbol === -1 || idxStrike === -1) {
      throw new Error("Required columns not found in FO data")
    }

    // Group strikes by symbol
    const symbolStrikes = {}

    for (const entry of entries) {
      const symbol = entry[idxSymbol].trim()
      const strikeRaw = entry[idxStrike]

      if (strikeRaw && strikeRaw > 0) {
        const strike = strikeRaw / 100.0 // convert to actual decimal strike

        if (!symbolStrikes[symbol]) {
          symbolStrikes[symbol] = new Set()
        }
        symbolStrikes[symbol].add(strike)
      }
    }

    // Calculate minimum non-zero strike gaps
    const strikeGaps = {}

    for (const [symbol, strikes] of Object.entries(symbolStrikes)) {
      const sortedStrikes = Array.from(strikes).sort((a, b) => a - b)
      const gaps = []

      for (let i = 1; i < sortedStrikes.length; i++) {
        const gap = Math.round((sortedStrikes[i] - sortedStrikes[i - 1]) * 1000000) / 1000000 // Round to 6 decimal places
        if (gap > 0) {
          gaps.push(gap)
        }
      }

      if (gaps.length > 0) {
        strikeGaps[symbol] = Math.min(...gaps)
      }
    }

    // Save to JSON file
    await fs.writeFile(strikeGapsPath, JSON.stringify(strikeGaps, null, 2))

    await writeLog(`Generated strikegaps.json with ${Object.keys(strikeGaps).length} symbols`)
  } catch (error) {
    await writeLog(`Error generating strike gaps: ${error.message}`)
    throw error
  }
}

function filterOptionData(data, symbol) {
  const header = data[0]
  const rows = data.slice(1)
  const df = rows
    .filter((r) => r[header.indexOf("sSymbol")].toUpperCase() === symbol.toUpperCase())
    .map((row) => ({
      id: `${row[header.indexOf("nMarketSegmentId")]}_${row[header.indexOf("nToken")]}`,
      expirydate: row[header.indexOf("ExpiryDate")],
      strikeprice: Number.parseFloat(row[header.indexOf("nStrikePrice")]) / 100,
      type: row[header.indexOf("sOptionType")],
    }))

  return df
}

async function generateDailyConfigs() {
  try {
    console.time("Config Generation")
    await writeLog("Starting daily config generation...")

    // Download and save NSE data locally
    const { foData, eqData } = await downloadAndSaveNSEData()

    // Generate additional JSON files (only once per day)
    await generateUnderlyingTokens(eqData, foData)
    await generateStrikeGaps(foData)

    const header = foData[0]
    const rows = foData.slice(1)

    // Extract unique symbols and filter out those matching xyz-(number) or ending with NSETEST
    const symbols = [...new Set(rows.map((row) => row[header.indexOf("sSymbol")]))].filter(
      (symbol) => !/^[a-zA-Z]+-\d+$/i.test(symbol) && !/NSETEST$/i.test(symbol),
    )

    await writeLog(`Found ${symbols.length} unique symbols in NSE_FO.json after filtering`)

    const symbolDefaultIds = {
      NIFTY: "1_26000",
      BANKNIFTY: "1_26009",
      FINNIFTY: "1_26037",
      SENSEX: "3_19000",
    }

    // Ensure config directory exists
    await fs.mkdir(configDir, { recursive: true })
    await writeLog(`Created/verified config directory: ${configDir}`)

    let totalConfigsGenerated = 0

    // Process symbols in parallel
    await Promise.all(
      symbols.map(async (symbol) => {
        const symbolDir = path.join(configDir, symbol)

        // Ensure directory exists first - moved to top and made more robust
        try {
          await fs.mkdir(symbolDir, { recursive: true })
          await writeLog(`Ensured directory exists: ${symbolDir}`)
        } catch (error) {
          await writeLog(`Failed to create directory ${symbolDir}: ${error.message}`)
          return // Skip this symbol if directory creation fails
        }

        // Clean up old config files
        try {
          const existingFiles = await fs.readdir(symbolDir).catch(() => [])
          await Promise.all(
            existingFiles
              .filter((file) => file.startsWith("config_") && file.endsWith(".txt"))
              .map((file) => fs.unlink(path.join(symbolDir, file))),
          )

          if (existingFiles.length > 0) {
            await writeLog(`Removed ${existingFiles.length} old configs in ${symbolDir}`)
          }
        } catch (error) {
          await writeLog(`Error cleaning up old configs in ${symbolDir}: ${error.message}`)
        }

        const defaultId = symbolDefaultIds[symbol] || `1_${Math.floor(Math.random() * 100000)}`
        const filteredData = filterOptionData(foData, symbol)
        const expiryDates = [...new Set(filteredData.map((d) => d.expirydate))]

        expiryDates.sort((a, b) => {
          const [aDay, aMonth, aYear] = a.split("-").map(Number)
          const [bDay, bMonth, bYear] = b.split("-").map(Number)
          return new Date(aYear, aMonth - 1, aDay) - new Date(bYear, bMonth - 1, bDay)
        })

        // Use 4 expiries for NIFTY, 2 for others
        const maxExpiries = symbol.toUpperCase() === "NIFTY" ? 4 : 2
        const nextExpiries = expiryDates.slice(0, maxExpiries)

        await writeLog(`${symbol}: Using next ${nextExpiries.length} expiry dates: ${nextExpiries.join(", ")}`)

        // Generate configs for each expiry
        for (const expiry of nextExpiries) {
          const subset = filteredData
            .filter((d) => d.expirydate === expiry)
            .sort((a, b) => a.strikeprice - b.strikeprice)

          const [dd, mm, yyyy] = expiry.split("-")
          const expiryClean = `${dd}${mm}${yyyy}`
          const filename = `config_${symbol}_${expiryClean}.txt`
          const filepath = path.join(symbolDir, filename)

          const idList = subset.map((d) => d.id)
          const combinedIds = [defaultId, ...idList].join(",")

          try {
            await fs.writeFile(filepath, combinedIds, "utf8")
            await writeLog(`Generated ${symbol}/${filename} with ${idList.length} option IDs`)
            totalConfigsGenerated++
          } catch (error) {
            await writeLog(`Failed to write ${filepath}: ${error.message}`)
          }
        }
      }),
    )

    // Generate mixed_config.txt by reading all individual configs AND underlying tokens
    const allTokens = []
    const dirEntries = await fs.readdir(configDir)
    const symbolsDirs = (
      await Promise.all(
        dirEntries.map(async (dir) => {
          const dirPath = path.join(configDir, dir)
          const stats = await fs.stat(dirPath)
          return stats.isDirectory() ? dir : null
        }),
      )
    ).filter((dir) => dir !== null)

    for (const symbol of symbolsDirs) {
      const symbolDir = path.join(configDir, symbol)
      const files = (await fs.readdir(symbolDir)).filter((f) => f.startsWith("config_") && f.endsWith(".txt"))

      for (const file of files) {
        const filepath = path.join(symbolDir, file)
        const tokens = (await fs.readFile(filepath, "utf8")).split(",").filter((token) => token.trim())
        allTokens.push(...tokens)
      }
    }

    // Add underlying tokens to mixed config
    const underlyingTokenPath = path.join(process.cwd(), UNDERLYING_TOKEN_FILE)
    const underlyingTokens = []

    // Add default underlying tokens first
    const defaultUnderlyingTokens = ["3_19000", "1_26000", "1_26037", "1_26009"]
    underlyingTokens.push(...defaultUnderlyingTokens)

    try {
      if (
        await fs
          .access(underlyingTokenPath)
          .then(() => true)
          .catch(() => false)
      ) {
        const underlyingData = JSON.parse(await fs.readFile(underlyingTokenPath, "utf8"))
        const additionalTokens = Object.values(underlyingData).filter((token) => token && token.trim())

        // Add additional tokens from file (avoid duplicates)
        for (const token of additionalTokens) {
          if (!underlyingTokens.includes(token)) {
            underlyingTokens.push(token)
          }
        }

        await writeLog(`Found ${additionalTokens.length} additional underlying tokens from file`)
      } else {
        await writeLog("underlyingtoken.json not found, using only default underlying tokens")
      }
    } catch (error) {
      await writeLog(`Error reading underlying tokens: ${error.message}`)
    }

    await writeLog(
      `Total underlying tokens: ${underlyingTokens.length} (including ${defaultUnderlyingTokens.length} default tokens)`,
    )

    // Add underlying tokens to allTokens
    allTokens.push(...underlyingTokens)

    // Create separate underlying config file
    const underlyingConfigPath = path.join(process.cwd(), UNDERLYING_CONFIG_FILE)
    await fs.writeFile(underlyingConfigPath, underlyingTokens.join(","), "utf8")
    await writeLog(`Generated ${UNDERLYING_CONFIG_FILE} with ${underlyingTokens.length} underlying tokens`)

    const mixedConfigPath = path.join(configDir, MIXED_CONFIG_FILE)
    const uniqueTokens = [...new Set(allTokens)] // Remove duplicates
    await fs.writeFile(mixedConfigPath, uniqueTokens.join(","), "utf8")

    console.timeEnd("Config Generation")
    await writeLog(
      `Generated ${MIXED_CONFIG_FILE} with ${uniqueTokens.length} unique tokens (including ${underlyingTokens.length} underlying tokens)`,
    )
    await writeLog(
      `Daily config generation complete! Generated ${totalConfigsGenerated} config files, 1 mixed config, and 1 underlying config`,
    )

    return uniqueTokens
  } catch (error) {
    await writeLog(`Error generating daily configs: ${error.message}`)
    throw error
  }
}

async function refreshAuthToken() {
  try {
    await writeLog("Refreshing Bearer token...")
    const authResponse = await axios.post(AUTH_URL, AUTH_CREDENTIALS, {
      headers: {
        "Content-Type": "application/json",
      },
      timeout: 10000,
    })

    if (authResponse.status !== 200) {
      throw new Error(`Auth failed: ${authResponse.status}`)
    }

    const token = authResponse.data.id_token || authResponse.data.token
    if (!token) {
      throw new Error("Token not found in response")
    }

    API_AUTH_TOKEN = token
    await writeLog(`Bearer token refreshed successfully`)
    return true
  } catch (error) {
    await writeLog(`Failed to refresh Bearer token: ${error.message}`)
    return false
  }
}

async function fetchTokenDataBatch(tokens) {
  if (!API_AUTH_TOKEN) {
    await writeLog("No Bearer token available, attempting to initialize...")
    const initialized = await initializeAuthToken()
    if (!initialized) {
      await writeLog("Failed to initialize Bearer token, cannot fetch token data")
      return []
    }
  }

  try {
    console.time(`API Request ${tokens.length}`)
    const payload = {
      taskcode: "100",
      entitycode: "700",
      body: {
        token: tokens.join(","),
      },
    }

    const taskHeaders = {
      "Content-Type": "application/json",
      Authorization: `Bearer ${API_AUTH_TOKEN}`,
    }

    const response = await axios.post(API_URL, payload, {
      headers: taskHeaders,
      timeout: 30000,
    })

    console.timeEnd(`API Request ${tokens.length}`)
    await writeLog(`API Batch Request: Status ${response.status} for ${tokens.length} tokens`)

    if (response.status === 200 && response.data) {
      let marketData = []

      // Log the full response structure for debugging
      await writeLog(`API Response structure: ${JSON.stringify(Object.keys(response.data))}`)

      if (response.data.data && Array.isArray(response.data.data)) {
        marketData = response.data.data
        await writeLog(`Found data in response.data.data: ${marketData.length} items`)
      } else if (Array.isArray(response.data)) {
        marketData = response.data
        await writeLog(`Found data in response root: ${marketData.length} items`)
      } else {
        // Try to find arrays in the response object
        for (const key in response.data) {
          if (Array.isArray(response.data[key])) {
            await writeLog(`Found array data in key: ${key} with ${response.data[key].length} items`)
            marketData = response.data[key]
            break
          }
        }

        // If still no data found, log the entire response for debugging
        if (marketData.length === 0) {
          await writeLog(`No array data found. Full response: ${JSON.stringify(response.data).substring(0, 500)}...`)
        }
      }

      await writeLog(`Processing ${marketData.length} market data packets...`)
      return marketData
    } else {
      await writeLog(`Unexpected response format or empty data. Status: ${response.status}`)
      return []
    }
  } catch (error) {
    await writeLog(`Error processing chunk: ${error.message}`)
    if (error.response) {
      await writeLog(`Response status: ${error.response.status}`)
      if (error.response.status === 401) {
        await writeLog("Token expired, attempting to refresh...")
        await refreshAuthToken()
      }
    }
    return []
  }
}

async function processBatchApiResponse(responseData) {
  if (!responseData || responseData.length === 0) {
    await writeLog(`SKIPPING: No valid batch data to process`)
    return { count: 0, sizeBytes: 0 }
  }

  try {
    console.time(`Redis Storage ${responseData.length}`)
    const pipeline = redisClient.multi()
    let batchCount = 0
    let batchSizeBytes = 0

    for (const packet of responseData) {
      if (packet && packet.nToken) {
        const redisKey = `2_${packet.nToken}`
        const packetString = JSON.stringify(packet)
        pipeline.set(redisKey, packetString)
        batchCount++
        batchSizeBytes += Buffer.byteLength(packetString, "utf8")
      }
    }

    if (batchCount > 0) {
      await pipeline.exec()
      timingSummary.totalPacketsStored += batchCount
      timingSummary.totalStorageSizeBytes += batchSizeBytes
      console.timeEnd(`Redis Storage ${responseData.length}`)
      await writeLog(`Stored ${batchCount} packets in Redis, total size: ${(batchSizeBytes / 1024).toFixed(2)} KB`)
    } else {
      await writeLog(`No valid packets found to store`)
    }

    return { count: batchCount, sizeBytes: batchSizeBytes }
  } catch (error) {
    await writeLog(`Error in batch Redis storage: ${error.message}`)
    return { count: 0, sizeBytes: 0 }
  }
}

async function continuousPolling(tokens) {
  if (!tokens || tokens.length === 0) {
    await writeLog("CONTINUOUS POLLING: No tokens available")
    return
  }

  try {
    console.time("Continuous Polling Cycle")
    const cycleStart = performance.now()

    // Clean tokens for API call
    const cleanTokens = []
    for (const entry of tokens) {
      const cleanEntry = entry.trim()
      if (cleanEntry.includes("_")) {
        const parts = cleanEntry.split("_", 2)
        if (parts.length === 2 && !isNaN(parts[1])) {
          cleanTokens.push(parts[1])
        }
      }
    }

    await writeLog(`CONTINUOUS POLLING: Starting cycle for ${cleanTokens.length} tokens`)

    let totalValidTokenCount = 0
    let totalCycleSizeBytes = 0

    // Process in batches for better performance
    const chunkSize = SUBSCRIBE_CHUNK_SIZE
    const numChunks = Math.ceil(cleanTokens.length / chunkSize)

    for (let i = 0; i < numChunks; i++) {
      const chunk = cleanTokens.slice(i * chunkSize, (i + 1) * chunkSize)
      const chunkNumber = i + 1
      let responseData = []
      let retryCount = 0
      const maxRetries = 2

      // Retry logic for failed chunks
      while (retryCount <= maxRetries) {
        responseData = await fetchTokenDataBatch(chunk)

        if (responseData.length > 0) {
          break // Success, exit retry loop
        } else if (retryCount < maxRetries) {
          retryCount++
          await writeLog(`Chunk ${chunkNumber} returned no data, retrying (${retryCount}/${maxRetries})...`)
          await new Promise((resolve) => setTimeout(resolve, 1000)) // Wait 1 second before retry
        } else {
          await writeLog(`Chunk ${chunkNumber} failed after ${maxRetries} retries, continuing with next chunk`)
          break // Exit retry loop and continue with processing (even with empty data)
        }
      }

      const { count, sizeBytes } = await processBatchApiResponse(responseData)
      totalValidTokenCount += count
      totalCycleSizeBytes += sizeBytes

      await writeLog(
        `Polling chunk ${chunkNumber}/${numChunks}: ${count} packets stored, size: ${(sizeBytes / 1024).toFixed(2)} KB`,
      )
    }

    const cycleTime = (performance.now() - cycleStart) / 1000
    console.timeEnd("Continuous Polling Cycle")

    await writeLog(
      `CONTINUOUS POLLING COMPLETE: ${totalValidTokenCount}/${cleanTokens.length} tokens processed in ${cycleTime.toFixed(2)}s, total size: ${(totalCycleSizeBytes / 1024).toFixed(2)} KB`,
    )

    await writeLog(`Total packets stored so far: ${timingSummary.totalPacketsStored}`)
    await writeLog(`Total storage size so far: ${(timingSummary.totalStorageSizeBytes / 1024 / 1024).toFixed(2)} MB`)
    await writeLog(`Next polling cycle in ${POLLING_INTERVAL / 1000} seconds`)
  } catch (err) {
    await writeLog(`CONTINUOUS POLLING ERROR: ${err.message}`)
  }
}

async function startContinuousPolling(tokens) {
  await writeLog(`STARTING CONTINUOUS POLLING: ${tokens.length} tokens every ${POLLING_INTERVAL / 1000} seconds`)

  // Initial poll
  await continuousPolling(tokens)

  // Set up interval for continuous polling
  pollingInterval = setInterval(async () => {
    await continuousPolling(tokens)
  }, POLLING_INTERVAL)

  await writeLog("CONTINUOUS POLLING: Interval established")
}

// Express app for API control
const app = express()
app.use(express.json())

// API endpoints
app.post("/regenerate-and-start", async (req, res) => {
  try {
    // Stop existing polling
    if (pollingInterval) {
      clearInterval(pollingInterval)
      pollingInterval = null
      await writeLog("Stopped existing polling interval")
    }

    // Generate fresh configs and start polling
    const tokens = await generateDailyConfigs()
    await startContinuousPolling(tokens)

    res.json({
      success: true,
      message: `Regenerated configs and started continuous polling for ${tokens.length} tokens`,
    })
  } catch (error) {
    res.status(500).json({ success: false, error: error.message })
  }
})

app.post("/stop-polling", async (req, res) => {
  try {
    if (pollingInterval) {
      clearInterval(pollingInterval)
      pollingInterval = null
      await writeLog("Stopped continuous polling")
      res.json({ success: true, message: "Stopped continuous polling" })
    } else {
      res.json({ success: false, message: "No polling was active" })
    }
  } catch (error) {
    res.status(500).json({ success: false, error: error.message })
  }
})

app.get("/status", async (req, res) => {
  try {
    const mixedConfigPath = path.join(configDir, MIXED_CONFIG_FILE)
    let tokenCount = 0

    try {
      const content = await fs.readFile(mixedConfigPath, "utf8")
      const tokens = content.split(",").filter((token) => token.trim())
      tokenCount = tokens.length
    } catch (error) {
      // Mixed config doesn't exist yet
    }

    // Check file generation status
    const fileStatus = {
      nse_fo: await isFileGeneratedToday(path.join(process.cwd(), NSE_FO_FILE)),
      nse_eq: await isFileGeneratedToday(path.join(process.cwd(), NSE_EQ_FILE)),
      underlying_tokens: await isFileGeneratedToday(path.join(process.cwd(), UNDERLYING_TOKEN_FILE)),
      strike_gaps: await isFileGeneratedToday(path.join(process.cwd(), STRIKE_GAPS_FILE)),
    }

    res.json({
      pollingActive: pollingInterval !== null,
      pollingInterval: POLLING_INTERVAL,
      tokenCount,
      currentISTDate: getISTDate(),
      filesGeneratedToday: fileStatus,
      performanceStats: {
        totalPacketsStored: timingSummary.totalPacketsStored,
        totalProcessingTime: timingSummary.totalProcessingTime,
        totalStorageSizeMB: (timingSummary.totalStorageSizeBytes / 1024 / 1024).toFixed(2),
        averageStorageRate:
          timingSummary.totalProcessingTime > 0
            ? (timingSummary.totalPacketsStored / (timingSummary.totalProcessingTime / 1000)).toFixed(2)
            : 0,
      },
    })
  } catch (error) {
    res.status(500).json({ success: false, error: error.message })
  }
})

// Start the server
app.listen(3001, async () => {
  await writeLog("API server running on port 3001")
  await writeLog("Endpoints:")
  await writeLog("  POST /regenerate-and-start - Generate configs and start continuous polling")
  await writeLog("  POST /stop-polling - Stop continuous polling")
  await writeLog("  GET /status - Get current status with performance stats")
  // Initialize Bearer token on server start
  await initializeAuthToken()
})

// Main initialization function
async function initialize() {
  try {
    console.time("Full Initialization")
    await writeLog("INITIALIZATION: Starting Simplified Option Chain API Client")
    // Generate configs and start continuous polling immediately
    const tokens = await generateDailyConfigs()
    await startContinuousPolling(tokens)
    console.timeEnd("Full Initialization")
    await writeLog("INITIALIZATION COMPLETE: System ready for continuous processing")
  } catch (error) {
    await writeLog(`INITIALIZATION FAILED: ${error.message}`)
    process.exit(1)
  }
}

// Graceful shutdown
process.on("SIGINT", async () => {
  await writeLog("SHUTDOWN: Received SIGINT, cleaning up...")
  if (pollingInterval) {
    clearInterval(pollingInterval)
    await writeLog("SHUTDOWN: Cleared polling interval")
  }
  // Final performance summary
  await writeLog("SHUTDOWN: Final Performance Summary")
  await writeLog(`Total packets stored: ${timingSummary.totalPacketsStored}`)
  await writeLog(`Total storage size: ${(timingSummary.totalStorageSizeBytes / 1024 / 1024).toFixed(2)} MB`)
  await writeLog(`Total processing time: ${(timingSummary.totalProcessingTime / 1000).toFixed(2)}s`)
  if (timingSummary.totalProcessingTime > 0) {
    await writeLog(
      `Average storage rate: ${(timingSummary.totalPacketsStored / (timingSummary.totalProcessingTime / 1000)).toFixed(2)} packets/second`,
    )
  }
  await writeLog("SHUTDOWN: Cleanup complete, exiting...")
  process.exit(0)
})

// Start initialization
initialize()
