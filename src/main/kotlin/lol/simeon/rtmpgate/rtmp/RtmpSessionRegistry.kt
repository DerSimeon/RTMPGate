package lol.simeon.rtmpgate.rtmp

import io.netty.channel.Channel
import kotlinx.serialization.Serializable
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class RtmpSessionRegistry {
    private val sessions = ConcurrentHashMap<String, ActiveRtmpSession>()

    fun register(channel: Channel): ActiveRtmpSession {
        val remoteAddress = channel.remoteAddress() as? InetSocketAddress
        val session = ActiveRtmpSession(
            id = UUID.randomUUID().toString(),
            remoteAddress = remoteAddress?.address?.hostAddress ?: "unknown",
            remotePort = remoteAddress?.port ?: 0,
            connectedAtEpochMillis = System.currentTimeMillis(),
            streamKey = null,
            target = null,
            state = "connected",
            channel = channel,
        )
        sessions[session.id] = session
        return session
    }

    fun updatePublish(id: String, streamKey: String, target: String?, state: String) {
        sessions.computeIfPresent(id) { _, session ->
            session.copy(streamKey = streamKey, target = target, state = state, channel = session.channel)
        }
    }

    fun updateState(id: String, state: String) {
        sessions.computeIfPresent(id) { _, session ->
            session.copy(state = state, channel = session.channel)
        }
    }

    fun unregister(id: String) {
        sessions.remove(id)
    }

    fun close(id: String): Boolean {
        val session = sessions[id] ?: return false
        session.channel.close()
        return true
    }

    fun count(): Int = sessions.size

    fun countByIp(ip: String): Int = sessions.values.count { it.remoteAddress == ip }

    fun list(): List<ActiveRtmpSessionResponse> {
        return sessions.values
            .sortedBy { it.connectedAtEpochMillis }
            .map { it.toResponse() }
    }
}

data class ActiveRtmpSession(
    val id: String,
    val remoteAddress: String,
    val remotePort: Int,
    val connectedAtEpochMillis: Long,
    val streamKey: String?,
    val target: String?,
    val state: String,
    val channel: Channel,
) {
    fun toResponse(): ActiveRtmpSessionResponse = ActiveRtmpSessionResponse(
        id = id,
        remoteAddress = remoteAddress,
        remotePort = remotePort,
        connectedAtEpochMillis = connectedAtEpochMillis,
        streamKey = streamKey,
        target = target,
        state = state,
    )
}

@Serializable
data class ActiveRtmpSessionResponse(
    val id: String,
    val remoteAddress: String,
    val remotePort: Int,
    val connectedAtEpochMillis: Long,
    val streamKey: String?,
    val target: String?,
    val state: String,
)
