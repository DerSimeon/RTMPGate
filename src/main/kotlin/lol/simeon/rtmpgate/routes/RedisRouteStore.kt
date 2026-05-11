package lol.simeon.rtmpgate.routes

import io.lettuce.core.RedisClient
import io.lettuce.core.ScanArgs
import io.lettuce.core.ScanCursor
import io.lettuce.core.api.StatefulRedisConnection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory

class RedisRouteStore(
    redisUrl: String,
) : RouteStore {
    private val logger = LoggerFactory.getLogger(RedisRouteStore::class.java)
    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }

    private val client: RedisClient = RedisClient.create(redisUrl)
    private val connection: StatefulRedisConnection<String, String> = client.connect()
    private val commands = connection.sync()

    override suspend fun get(streamKey: String): RouteRecord? = withContext(Dispatchers.IO) {
        decode(streamKey, commands.get(key(streamKey)))
    }

    override suspend fun list(): List<RouteRecord> = withContext(Dispatchers.IO) {
        val records = mutableListOf<RouteRecord>()
        var cursor: ScanCursor = ScanCursor.INITIAL
        val args = ScanArgs.Builder.matches("route:*").limit(500)

        do {
            val result = commands.scan(cursor, args)
            cursor = result
            for (redisKey in result.keys) {
                val streamKey = redisKey.removePrefix("route:")
                decode(streamKey, commands.get(redisKey))?.let(records::add)
            }
        } while (!cursor.isFinished)

        records.sortedBy { it.streamKey }
    }

    override suspend fun put(record: RouteRecord) {
        withContext(Dispatchers.IO) {
            commands.set(key(record.streamKey), json.encodeToString(record))
        }
    }

    override suspend fun delete(streamKey: String): Boolean = withContext(Dispatchers.IO) {
        commands.del(key(streamKey)) > 0
    }

    override suspend fun isReady(): Boolean = withContext(Dispatchers.IO) {
        runCatching { commands.ping() == "PONG" }.getOrDefault(false)
    }

    override fun close() {
        runCatching { connection.close() }
        runCatching { client.shutdown() }
    }

    private fun decode(streamKey: String, raw: String?): RouteRecord? {
        return raw?.let {
            runCatching { json.decodeFromString<RouteRecord>(it) }
                .onFailure { error -> logger.warn("Invalid route payload for stream key {}", streamKey, error) }
                .getOrNull()
        }
    }

    private fun key(streamKey: String): String = "route:$streamKey"
}
