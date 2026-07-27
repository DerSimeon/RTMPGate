package lol.simeon.rtmpgate

import lol.simeon.rtmpgate.config.AppConfig
import java.net.ServerSocket

fun testConfig(
    httpPort: Int = freePort(),
    rtmpPort: Int = freePort(),
    rtmpDebug: Boolean = false,
    adminToken: String? = null,
    requireReadAuth: Boolean = false,
): AppConfig = AppConfig(
    httpHost = "127.0.0.1",
    httpPort = httpPort,
    rtmpHost = "127.0.0.1",
    rtmpPort = rtmpPort,
    storageBackend = "memory",
    redisUrl = "redis://localhost:6379",
    routeCacheSeconds = 1,
    maxRtmpChunkSize = 65_536,
    connectTimeoutMillis = 2_000,
    readTimeoutMillis = 5_000,
    rtmpDebug = rtmpDebug,
    adminToken = adminToken,
    requireReadAuth = requireReadAuth,
    maxActiveSessions = 0,
    maxSessionsPerIp = 0,
    targetHostAllowList = emptySet(),
    maxInputBufferBytes = 8 * 1024 * 1024,
    maxRtmpMessageBytes = 16 * 1024 * 1024,
    gracefulShutdownMillis = 1_000,
    startupBufferBytes = 64L * 1024L * 1024L,
    startupBufferMessages = 4096,
    upstreamHighWatermarkBytes = 16 * 1024 * 1024,
    upstreamLowWatermarkBytes = 8 * 1024 * 1024,
    backpressureTimeoutMillis = 10_000,
    upstreamWriteTimeoutMillis = 10_000,
)

fun freePort(): Int = ServerSocket(0).use { it.localPort }
