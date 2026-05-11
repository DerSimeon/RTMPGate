package lol.simeon.rtmpgate.rtmp

import io.netty.buffer.ByteBuf
import java.nio.ByteBuffer

object RtmpServerResponses {
    // Keep all server responses chunked at RTMP's default 128 bytes.
    // The client only accepts larger server chunks after it has received and applied
    // a server-side Set Chunk Size message. Until then, writing >128-byte chunks
    // corrupts the RTMP stream from the client's point of view.
    private val codec = RtmpChunkCodec()

    fun windowAcknowledgementSize(size: Int = RtmpConstants.DEFAULT_WINDOW_ACK_SIZE): ByteBuf {
        return codec.encode(
            RtmpMessage(0, RtmpConstants.MSG_WINDOW_ACKNOWLEDGEMENT_SIZE, 0, ByteBuffer.allocate(4).putInt(size).array()),
            chunkStreamId = 2,
        )
    }

    fun peerBandwidth(size: Int = RtmpConstants.DEFAULT_WINDOW_ACK_SIZE, limitType: Int = 2): ByteBuf {
        return codec.encode(
            RtmpMessage(0, RtmpConstants.MSG_SET_PEER_BANDWIDTH, 0, ByteBuffer.allocate(5).putInt(size).put(limitType.toByte()).array()),
            chunkStreamId = 2,
        )
    }

    fun setChunkSize(size: Int = 4096): ByteBuf {
        return codec.encode(
            RtmpMessage(0, RtmpConstants.MSG_SET_CHUNK_SIZE, 0, ByteBuffer.allocate(4).putInt(size).array()),
            chunkStreamId = 2,
        )
    }

    fun streamBegin(streamId: Int): ByteBuf {
        val payload = ByteBuffer.allocate(6)
            .putShort(RtmpConstants.USER_CONTROL_STREAM_BEGIN.toShort())
            .putInt(streamId)
            .array()
        return codec.encode(RtmpMessage(0, RtmpConstants.MSG_USER_CONTROL, 0, payload), chunkStreamId = 2)
    }

    fun connectResult(transactionId: Double): ByteBuf {
        // Keep this deliberately below the default 128 byte RTMP chunk size.
        // Some publishers do not reliably continue the connect flow if the first
        // NetConnection _result arrives chunked or after a server-side chunk-size change.
        val payload = RtmpAmf0.concat(
            RtmpAmf0.string("_result"),
            RtmpAmf0.number(transactionId),
            RtmpAmf0.obj(),
            RtmpAmf0.obj(
                "level" to RtmpAmf0.string("status"),
                "code" to RtmpAmf0.string("NetConnection.Connect.Success"),
            ),
        )
        return codec.encode(
            RtmpMessage(0, RtmpConstants.MSG_COMMAND_AMF0, 0, payload),
            chunkStreamId = 3,
        )
    }

    fun createStreamResult(transactionId: Double, streamId: Int): ByteBuf {
        val payload = RtmpAmf0.concat(
            RtmpAmf0.string("_result"),
            RtmpAmf0.number(transactionId),
            RtmpAmf0.nullValue(),
            RtmpAmf0.number(streamId.toDouble()),
        )
        return codec.encode(
            RtmpMessage(0, RtmpConstants.MSG_COMMAND_AMF0, 0, payload),
            chunkStreamId = 3,
        )
    }

    fun onFCPublish(transactionId: Double, streamKey: String?): ByteBuf {
        val payload = RtmpAmf0.concat(
            RtmpAmf0.string("onFCPublish"),
            RtmpAmf0.number(transactionId),
            RtmpAmf0.nullValue(),
            RtmpAmf0.obj(
                "code" to RtmpAmf0.string("NetStream.Publish.Start"),
                "description" to RtmpAmf0.string("FCPublish to stream is successful."),
                "details" to RtmpAmf0.string(streamKey ?: ""),
            ),
        )
        return codec.encode(
            RtmpMessage(0, RtmpConstants.MSG_COMMAND_AMF0, 0, payload),
            chunkStreamId = 3,
        )
    }

    fun publishStart(streamId: Int, streamKey: String): ByteBuf {
        val payload = RtmpAmf0.concat(
            RtmpAmf0.string("onStatus"),
            RtmpAmf0.number(0.0),
            RtmpAmf0.nullValue(),
            RtmpAmf0.obj(
                "level" to RtmpAmf0.string("status"),
                "code" to RtmpAmf0.string("NetStream.Publish.Start"),
                "description" to RtmpAmf0.string("Start publishing."),
                "details" to RtmpAmf0.string(streamKey),
            ),
        )
        return codec.encode(
            RtmpMessage(0, RtmpConstants.MSG_COMMAND_AMF0, streamId, payload),
            chunkStreamId = 5,
        )
    }

    fun publishRejected(streamId: Int, streamKey: String): ByteBuf {
        val payload = RtmpAmf0.concat(
            RtmpAmf0.string("onStatus"),
            RtmpAmf0.number(0.0),
            RtmpAmf0.nullValue(),
            RtmpAmf0.obj(
                "level" to RtmpAmf0.string("error"),
                "code" to RtmpAmf0.string("NetStream.Publish.BadName"),
                "description" to RtmpAmf0.string("Unknown stream key."),
                "details" to RtmpAmf0.string(streamKey),
            ),
        )
        return codec.encode(
            RtmpMessage(0, RtmpConstants.MSG_COMMAND_AMF0, streamId, payload),
            chunkStreamId = 5,
        )
    }
}
