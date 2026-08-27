package lol.simeon.rtmpgate.rtmp

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import kotlin.math.min

class RtmpChunkCodec(
    private var inboundChunkSize: Int = RtmpConstants.DEFAULT_CHUNK_SIZE,
    private val maxMessageBytes: Int = DEFAULT_MAX_MESSAGE_BYTES,
) {
    private val streams = mutableMapOf<Int, RtmpChunkStreamState>()

    fun readPackets(input: ByteBuf): List<RtmpPacket> {
        return readMessages(input).map(RtmpPacket::fromMessage)
    }

    fun readMessages(input: ByteBuf): List<RtmpMessage> {
        val messages = mutableListOf<RtmpMessage>()

        loop@ while (input.isReadable) {
            input.markReaderIndex()
            when (val result = parseNext(input)) {
                RtmpChunkReadResult.Incomplete -> {
                    input.resetReaderIndex()
                    break@loop
                }

                RtmpChunkReadResult.Partial -> Unit

                is RtmpChunkReadResult.Message -> {
                    messages += result.message
                    updateInboundChunkSize(result.message)
                }
            }
        }

        return messages
    }

    fun command(message: RtmpMessage): RtmpCommand? = RtmpAmf0.parseCommand(message)

    fun encodePacket(
        packet: RtmpPacket,
        chunkStreamId: Int,
        outboundChunkSize: Int = RtmpConstants.DEFAULT_CHUNK_SIZE,
    ): ByteBuf {
        val payload = packet.payload.retainedDuplicate()
        val out = Unpooled.buffer(packet.payloadSize + 32)
        var first = true

        try {
            while (payload.isReadable) {
                val bytesToWrite = min(outboundChunkSize, payload.readableBytes())
                writeChunkHeader(out, packet, chunkStreamId, first)
                out.writeBytes(payload, bytesToWrite)
                first = false
            }
        } finally {
            payload.release()
        }

        return out
    }

    fun encode(
        message: RtmpMessage,
        chunkStreamId: Int,
        outboundChunkSize: Int = RtmpConstants.DEFAULT_CHUNK_SIZE,
    ): ByteBuf {
        val payload = Unpooled.wrappedBuffer(message.payload)
        val out = Unpooled.buffer(message.payload.size + 32)
        var first = true

        try {
            while (payload.isReadable) {
                val bytesToWrite = min(outboundChunkSize, payload.readableBytes())
                writeChunkHeader(out, message, chunkStreamId, first)
                out.writeBytes(payload, bytesToWrite)
                first = false
            }
        } finally {
            payload.release()
        }

        return out
    }

    private fun parseNext(input: ByteBuf): RtmpChunkReadResult {
        val chunkStart = readChunkStart(input) ?: return RtmpChunkReadResult.Incomplete
        val previous = streams[chunkStart.chunkStreamId]
        val header = readHeader(input, chunkStart.format, previous) ?: return RtmpChunkReadResult.Incomplete
        val effectiveHeader = readExtendedTimestamp(input, chunkStart.format, previous, header)
            ?: return RtmpChunkReadResult.Incomplete
        val state = nextState(chunkStart.format, previous, effectiveHeader)
        return readPayload(input, chunkStart.chunkStreamId, state)
    }

    private fun readChunkStart(input: ByteBuf): ChunkStart? {
        if (input.readableBytes() < 1) return null

        val first = input.readUnsignedByte().toInt()
        val format = first shr 6
        val rawChunkStreamId = first and 0x3f
        val chunkStreamId = readChunkStreamId(input, rawChunkStreamId) ?: return null

        return ChunkStart(format = format, chunkStreamId = chunkStreamId)
    }

    private fun readChunkStreamId(input: ByteBuf, rawChunkStreamId: Int): Int? {
        return when (rawChunkStreamId) {
            0 -> readOneByteChunkStreamId(input)
            1 -> readTwoByteChunkStreamId(input)
            else -> rawChunkStreamId
        }
    }

    private fun readOneByteChunkStreamId(input: ByteBuf): Int? {
        if (input.readableBytes() < 1) return null
        return input.readUnsignedByte().toInt() + 64
    }

    private fun readTwoByteChunkStreamId(input: ByteBuf): Int? {
        if (input.readableBytes() < 2) return null

        val low = input.readUnsignedByte().toInt()
        val high = input.readUnsignedByte().toInt()

        return 64 + low + high * 256
    }

    private fun readHeader(
        input: ByteBuf,
        format: Int,
        previous: RtmpChunkStreamState?,
    ): RtmpChunkHeader? {
        return when (format) {
            0 -> readType0Header(input)
            1 -> readType1Header(input, previous)
            2 -> readType2Header(input, previous)
            3 -> previous?.header
            else -> null
        }
    }

    private fun readExtendedTimestamp(
        input: ByteBuf,
        format: Int,
        previous: RtmpChunkStreamState?,
        header: RtmpChunkHeader,
    ): RtmpChunkHeader? {
        if (!header.extendedTimestamp) return header
        if (input.readableBytes() < 4) return null

        val extendedTimestamp = input.readInt()

        return when (format) {
            0 -> header.copy(timestamp = extendedTimestamp)
            1, 2 -> header.copy(
                timestamp = previous?.header?.timestamp?.plus(extendedTimestamp) ?: return null,
                timestampDelta = extendedTimestamp,
            )
            // A type-3 chunk repeats the extended field required by the inherited header.
            // Its value does not change the already decoded header state: continuations keep
            // their message timestamp, while new messages apply the inherited delta below.
            3 -> header
            else -> null
        }
    }

    private fun nextState(
        format: Int,
        previous: RtmpChunkStreamState?,
        header: RtmpChunkHeader,
    ): RtmpChunkStreamState {
        return when {
            format == 3 && previous != null && !previous.complete -> previous
            format == 3 && previous != null -> RtmpChunkStreamState(
                header = header.copy(timestamp = header.timestamp + header.timestampDelta),
            )
            else -> RtmpChunkStreamState(header = header)
        }
    }

    private fun readPayload(
        input: ByteBuf,
        chunkStreamId: Int,
        state: RtmpChunkStreamState,
    ): RtmpChunkReadResult {
        // Reject an over-large declared message length before we start accumulating its payload
        // on the heap. Prevents a malicious/broken peer from exhausting memory.
        check(state.header.length <= maxMessageBytes) {
            "RTMP message length ${state.header.length} exceeds limit $maxMessageBytes"
        }

        val bytesToRead = bytesToRead(state) ?: return RtmpChunkReadResult.Incomplete
        if (input.readableBytes() < bytesToRead) return RtmpChunkReadResult.Incomplete

        copyPayloadBytes(input, state, bytesToRead)

        return if (state.payload.size() < state.header.length) {
            streams[chunkStreamId] = state.copy(complete = false)
            RtmpChunkReadResult.Partial
        } else {
            completeMessage(chunkStreamId, state)
        }
    }

    private fun bytesToRead(state: RtmpChunkStreamState): Int? {
        val missing = state.header.length - state.payload.size()
        if (missing < 0) return null

        return min(missing, inboundChunkSize)
    }

    private fun copyPayloadBytes(input: ByteBuf, state: RtmpChunkStreamState, bytesToRead: Int) {
        val bytes = ByteArray(bytesToRead)
        input.readBytes(bytes)
        state.payload.write(bytes)
    }

    private fun completeMessage(chunkStreamId: Int, state: RtmpChunkStreamState): RtmpChunkReadResult.Message {
        val message = RtmpMessage(
            timestamp = state.header.timestamp,
            typeId = state.header.typeId,
            streamId = state.header.streamId,
            payload = state.payload.toByteArray(),
        )

        streams[chunkStreamId] = RtmpChunkStreamState(header = state.header, complete = true)

        return RtmpChunkReadResult.Message(message)
    }

    private fun updateInboundChunkSize(message: RtmpMessage) {
        if (message.typeId == RtmpConstants.MSG_SET_CHUNK_SIZE && message.payload.size >= Int.SIZE_BYTES) {
            inboundChunkSize = Unpooled.wrappedBuffer(message.payload).use { it.readInt() }
        }
    }

    private fun writeChunkHeader(
        out: ByteBuf,
        message: RtmpMessage,
        chunkStreamId: Int,
        first: Boolean,
    ) {
        if (first) {
            writeType0ChunkHeader(out, message, chunkStreamId)
        } else {
            writeType3ChunkHeader(out, message, chunkStreamId)
        }
    }

    private fun writeChunkHeader(
        out: ByteBuf,
        packet: RtmpPacket,
        chunkStreamId: Int,
        first: Boolean,
    ) {
        if (first) {
            writeType0ChunkHeader(out, packet, chunkStreamId)
        } else {
            writeType3ChunkHeader(out, packet, chunkStreamId)
        }
    }

    private fun writeType0ChunkHeader(out: ByteBuf, message: RtmpMessage, chunkStreamId: Int) {
        writeBasicHeader(out, 0, chunkStreamId)

        val timestamp = min(message.timestamp, MAX_BASIC_TIMESTAMP)
        out.writeMedium(timestamp)
        out.writeMedium(message.payload.size)
        out.writeByte(message.typeId)
        out.writeIntLE(message.streamId)

        if (message.timestamp >= MAX_BASIC_TIMESTAMP) {
            out.writeInt(message.timestamp)
        }
    }

    private fun writeType0ChunkHeader(out: ByteBuf, packet: RtmpPacket, chunkStreamId: Int) {
        writeBasicHeader(out, 0, chunkStreamId)

        val timestamp = min(packet.timestamp, MAX_BASIC_TIMESTAMP)
        out.writeMedium(timestamp)
        out.writeMedium(packet.payloadSize)
        out.writeByte(packet.typeId)
        out.writeIntLE(packet.streamId)

        if (packet.timestamp >= MAX_BASIC_TIMESTAMP) {
            out.writeInt(packet.timestamp)
        }
    }

    private fun writeType3ChunkHeader(out: ByteBuf, message: RtmpMessage, chunkStreamId: Int) {
        writeBasicHeader(out, 3, chunkStreamId)

        if (message.timestamp >= MAX_BASIC_TIMESTAMP) {
            out.writeInt(message.timestamp)
        }
    }

    private fun writeType3ChunkHeader(out: ByteBuf, packet: RtmpPacket, chunkStreamId: Int) {
        writeBasicHeader(out, 3, chunkStreamId)

        if (packet.timestamp >= MAX_BASIC_TIMESTAMP) {
            out.writeInt(packet.timestamp)
        }
    }

    private fun readType0Header(input: ByteBuf): RtmpChunkHeader? {
        if (input.readableBytes() < TYPE_0_HEADER_SIZE) return null

        val timestamp = input.readUnsignedMedium()

        return RtmpChunkHeader(
            timestamp = timestamp,
            timestampDelta = 0,
            length = input.readUnsignedMedium(),
            typeId = input.readUnsignedByte().toInt(),
            streamId = input.readIntLE(),
            extendedTimestamp = timestamp == MAX_BASIC_TIMESTAMP,
        )
    }

    private fun readType1Header(input: ByteBuf, previous: RtmpChunkStreamState?): RtmpChunkHeader? {
        if (previous == null || input.readableBytes() < TYPE_1_HEADER_SIZE) return null

        val delta = input.readUnsignedMedium()

        return RtmpChunkHeader(
            timestamp = previous.header.timestamp + delta,
            timestampDelta = delta,
            length = input.readUnsignedMedium(),
            typeId = input.readUnsignedByte().toInt(),
            streamId = previous.header.streamId,
            extendedTimestamp = delta == MAX_BASIC_TIMESTAMP,
        )
    }

    private fun readType2Header(input: ByteBuf, previous: RtmpChunkStreamState?): RtmpChunkHeader? {
        if (previous == null || input.readableBytes() < TYPE_2_HEADER_SIZE) return null

        val delta = input.readUnsignedMedium()

        return previous.header.copy(
            timestamp = previous.header.timestamp + delta,
            timestampDelta = delta,
            extendedTimestamp = delta == MAX_BASIC_TIMESTAMP,
        )
    }

    private fun writeBasicHeader(out: ByteBuf, format: Int, chunkStreamId: Int) {
        require(chunkStreamId in 2..63) { "Only small RTMP chunk stream ids are supported by this encoder" }
        out.writeByte((format shl 6) or chunkStreamId)
    }

    private data class ChunkStart(
        val format: Int,
        val chunkStreamId: Int,
    )

    private companion object {
        const val MAX_BASIC_TIMESTAMP = 0x00ff_ffff
        const val TYPE_0_HEADER_SIZE = 11
        const val TYPE_1_HEADER_SIZE = 7
        const val TYPE_2_HEADER_SIZE = 3
        const val DEFAULT_MAX_MESSAGE_BYTES = 16 * 1024 * 1024
    }
}

private inline fun <T> ByteBuf.use(block: (ByteBuf) -> T): T {
    return try {
        block(this)
    } finally {
        release()
    }
}
