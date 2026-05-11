package lol.simeon.rtmpgate.rtmp

data class RtmpPublishInfo(
    val app: String,
    val streamKey: String,
    val clientStreamId: Int,
)
