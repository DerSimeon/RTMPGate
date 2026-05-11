package lol.simeon.rtmpgate.rtmp

data class RtmpCommand(
    val name: String,
    val transactionId: Double,
    val app: String? = null,
    val streamKey: String? = null,
)
