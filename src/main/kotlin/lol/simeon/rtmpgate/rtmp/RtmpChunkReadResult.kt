package lol.simeon.rtmpgate.rtmp

sealed interface RtmpChunkReadResult {
    data object Incomplete : RtmpChunkReadResult
    data object Partial : RtmpChunkReadResult
    data class Message(val message: RtmpMessage) : RtmpChunkReadResult
}
