package lol.simeon.rtmpgate.routes

import kotlinx.serialization.Serializable

@Serializable
data class RouteRecord(
    val streamKey: String,
    val target: String,
    val createdAtEpochMillis: Long,
    val updatedAtEpochMillis: Long,
)

interface RouteStore : AutoCloseable {
    suspend fun get(streamKey: String): RouteRecord?
    suspend fun list(): List<RouteRecord>
    suspend fun put(record: RouteRecord)
    suspend fun delete(streamKey: String): Boolean
    suspend fun isReady(): Boolean = true
    override fun close() {}
}
