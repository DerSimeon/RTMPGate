package lol.simeon.rtmpgate.config

data class AppConfig(
    val httpHost: String,
    val httpPort: Int,
    val rtmpHost: String,
    val rtmpPort: Int,
    val storageBackend: String,
    val redisUrl: String,
    val routeCacheSeconds: Long,
    val maxRtmpChunkSize: Int,
    val connectTimeoutMillis: Int,
    val readTimeoutMillis: Int,
    val rtmpDebug: Boolean,
    val adminToken: String?,
    val maxActiveSessions: Int,
    val maxSessionsPerIp: Int,
    val targetHostAllowList: Set<String>,
    val gracefulShutdownMillis: Long,
) {
    companion object {
        fun fromEnvironment(env: Map<String, String> = System.getenv()): AppConfig {
            val reader = EnvironmentReader(env)

            return AppConfig(
                httpHost = reader.string("RTMPGATE_HTTP_HOST", "0.0.0.0"),
                httpPort = reader.int("RTMPGATE_HTTP_PORT", 8080),
                rtmpHost = reader.string("RTMPGATE_RTMP_HOST", "0.0.0.0"),
                rtmpPort = reader.int("RTMPGATE_RTMP_PORT", 1935),
                storageBackend = reader.string("RTMPGATE_STORAGE", "redis"),
                redisUrl = reader.string("RTMPGATE_REDIS_URL", "redis://localhost:6379"),
                routeCacheSeconds = reader.long("RTMPGATE_ROUTE_CACHE_SECONDS", 5),
                maxRtmpChunkSize = reader.int("RTMPGATE_MAX_RTMP_CHUNK_SIZE", 65_536),
                connectTimeoutMillis = reader.int("RTMPGATE_CONNECT_TIMEOUT_MS", 5_000),
                readTimeoutMillis = reader.int("RTMPGATE_READ_TIMEOUT_MS", 30_000),
                rtmpDebug = reader.boolean("RTMPGATE_RTMP_DEBUG"),
                adminToken = reader.optionalString("RTMPGATE_ADMIN_TOKEN"),
                maxActiveSessions = reader.int("RTMPGATE_MAX_ACTIVE_SESSIONS", 0),
                maxSessionsPerIp = reader.int("RTMPGATE_MAX_SESSIONS_PER_IP", 0),
                targetHostAllowList = reader.csvSet("RTMPGATE_TARGET_HOST_ALLOWLIST"),
                gracefulShutdownMillis = reader.long("RTMPGATE_GRACEFUL_SHUTDOWN_MS", 15_000),
            )
        }
    }
}

private class EnvironmentReader(
    private val env: Map<String, String>,
) {
    fun string(name: String, default: String): String = env[name] ?: default

    fun optionalString(name: String): String? = env[name]?.trim()?.takeIf { it.isNotEmpty() }

    fun int(name: String, default: Int): Int = env[name]?.toIntOrNull() ?: default

    fun long(name: String, default: Long): Long = env[name]?.toLongOrNull() ?: default

    fun boolean(name: String): Boolean = env[name]?.equals("true", ignoreCase = true) == true

    fun csvSet(name: String): Set<String> {
        return env[name]
            ?.split(',')
            ?.map { it.trim().lowercase() }
            ?.filter { it.isNotEmpty() }
            ?.toSet()
            ?: emptySet()
    }
}
