package lol.simeon.rtmpgate.rtmp

data class RtmpChunkHeader(
    val timestamp: Int,
    val length: Int,
    val typeId: Int,
    val streamId: Int,
    val extendedTimestamp: Boolean,
)
