package lol.simeon.rtmpgate.rtmp

data class RtmpMessage(
    val timestamp: Int,
    val typeId: Int,
    val streamId: Int,
    val payload: ByteArray,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RtmpMessage) return false
        return timestamp == other.timestamp &&
            typeId == other.typeId &&
            streamId == other.streamId &&
            payload.contentEquals(other.payload)
    }

    override fun hashCode(): Int {
        var result = timestamp
        result = 31 * result + typeId
        result = 31 * result + streamId
        result = 31 * result + payload.contentHashCode()
        return result
    }
}
