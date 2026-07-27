package lol.simeon.rtmpgate.rtmp

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled

/**
 * ByteBuf-backed RTMP packet representation used on the media relay path.
 *
 * The current chunk assembler still has to reassemble RTMP chunks into a contiguous payload.
 * Keeping the reassembled payload as ByteBuf avoids the older RtmpMessage-only relay API and
 * gives the upstream writer direct access to readable bytes without exposing media as AMF data.
 */
data class RtmpPacket(
    val timestamp: Int,
    val typeId: Int,
    val streamId: Int,
    val payload: ByteBuf,
) {
    val payloadSize: Int
        get() = payload.readableBytes()

    fun retainedCopyWithStreamId(streamId: Int): RtmpPacket {
        return copy(streamId = streamId, payload = payload.retainedDuplicate())
    }

    fun toMessage(): RtmpMessage {
        val duplicate = payload.duplicate()
        val bytes = ByteArray(duplicate.readableBytes())
        duplicate.readBytes(bytes)
        return RtmpMessage(
            timestamp = timestamp,
            typeId = typeId,
            streamId = streamId,
            payload = bytes,
        )
    }

    fun release() {
        if (payload.refCnt() > 0) {
            payload.release()
        }
    }

    companion object {
        fun fromMessage(message: RtmpMessage): RtmpPacket = RtmpPacket(
            timestamp = message.timestamp,
            typeId = message.typeId,
            streamId = message.streamId,
            payload = Unpooled.wrappedBuffer(message.payload),
        )
    }
}
