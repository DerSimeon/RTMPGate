package lol.simeon.rtmpgate.rtmp

import java.io.ByteArrayOutputStream

data class RtmpChunkStreamState(
    val header: RtmpChunkHeader,
    val payload: ByteArrayOutputStream = ByteArrayOutputStream(header.length),
    val complete: Boolean = false,
)
