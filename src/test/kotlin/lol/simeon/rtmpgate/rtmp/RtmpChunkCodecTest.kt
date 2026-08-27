package lol.simeon.rtmpgate.rtmp

import io.netty.buffer.Unpooled
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RtmpChunkCodecTest {
    @Test
    fun `type 3 new messages reuse the preceding type 2 timestamp delta`() {
        val input = Unpooled.buffer()
        writeType0(input, chunkStreamId = 6, timestamp = 1000, payload = byteArrayOf(1))
        writeType2(input, chunkStreamId = 6, timestampDelta = 20, payload = byteArrayOf(2))
        writeType3(input, chunkStreamId = 6, payload = byteArrayOf(3))
        writeType3(input, chunkStreamId = 6, payload = byteArrayOf(4))

        val messages = RtmpChunkCodec().readMessages(input)

        assertEquals(listOf(1000, 1020, 1040, 1060), messages.map(RtmpMessage::timestamp))
        input.release()
    }

    @Test
    fun `type 3 continuation chunks retain one message timestamp`() {
        val input = Unpooled.buffer()
        val payload = byteArrayOf(1, 2, 3, 4, 5, 6)
        writeType0(input, chunkStreamId = 6, timestamp = 100, payload = byteArrayOf(9))
        writeType1(
            input,
            chunkStreamId = 6,
            timestampDelta = 25,
            messageLength = payload.size,
            payload = payload.copyOfRange(0, 2),
        )
        writeType3(input, chunkStreamId = 6, payload = payload.copyOfRange(2, 4))
        writeType3(input, chunkStreamId = 6, payload = payload.copyOfRange(4, 6))

        val messages = RtmpChunkCodec(inboundChunkSize = 2).readMessages(input)

        assertEquals(listOf(100, 125), messages.map(RtmpMessage::timestamp))
        assertContentEquals(payload, messages.last().payload)
        input.release()
    }

    @Test
    fun `type 3 new messages reuse the preceding type 1 timestamp delta`() {
        val input = Unpooled.buffer()
        writeType0(input, chunkStreamId = 6, timestamp = 500, payload = byteArrayOf(1))
        writeType1(input, chunkStreamId = 6, timestampDelta = 30, payload = byteArrayOf(2))
        writeType3(input, chunkStreamId = 6, payload = byteArrayOf(3))
        writeType3(input, chunkStreamId = 6, payload = byteArrayOf(4))

        val messages = RtmpChunkCodec().readMessages(input)

        assertEquals(listOf(500, 530, 560, 590), messages.map(RtmpMessage::timestamp))
        input.release()
    }

    @Test
    fun `type 0 resets the timestamp delta inherited by a type 3 new message`() {
        val input = Unpooled.buffer()
        writeType0(input, chunkStreamId = 6, timestamp = 1000, payload = byteArrayOf(1))
        writeType3(input, chunkStreamId = 6, payload = byteArrayOf(2))

        val messages = RtmpChunkCodec().readMessages(input)

        assertEquals(listOf(1000, 1000), messages.map(RtmpMessage::timestamp))
        input.release()
    }

    @Test
    fun `type 0 extended timestamp is decoded as an absolute timestamp`() {
        val input = Unpooled.buffer()
        val timestamp = 0x0100_0000
        writeType0(input, chunkStreamId = 6, timestamp = timestamp, payload = byteArrayOf(1))

        val message = RtmpChunkCodec().readMessages(input).single()

        assertEquals(timestamp, message.timestamp)
        input.release()
    }

    @Test
    fun `extended delta is applied once across type 3 continuation and new message chunks`() {
        val input = Unpooled.buffer()
        val timestampDelta = 0x0100_0000
        writeType0(input, chunkStreamId = 6, timestamp = 100, payload = byteArrayOf(0))
        writeType1(
            input,
            chunkStreamId = 6,
            timestampDelta = timestampDelta,
            messageLength = 3,
            payload = byteArrayOf(1, 2),
        )
        writeType3(
            input,
            chunkStreamId = 6,
            extendedTimestamp = timestampDelta,
            payload = byteArrayOf(3),
        )
        writeType3(
            input,
            chunkStreamId = 6,
            extendedTimestamp = timestampDelta,
            payload = byteArrayOf(4, 5),
        )
        writeType3(
            input,
            chunkStreamId = 6,
            extendedTimestamp = timestampDelta,
            payload = byteArrayOf(6),
        )

        val messages = RtmpChunkCodec(inboundChunkSize = 2).readMessages(input)

        assertEquals(
            listOf(100, 100 + timestampDelta, 100 + timestampDelta * 2),
            messages.map(RtmpMessage::timestamp),
        )
        assertContentEquals(byteArrayOf(1, 2, 3), messages[1].payload)
        assertContentEquals(byteArrayOf(4, 5, 6), messages[2].payload)
        input.release()
    }

    @Test
    fun `interleaved chunk streams retain independent timestamp histories`() {
        val input = Unpooled.buffer()
        writeType0(
            input,
            chunkStreamId = 6,
            timestamp = 1000,
            typeId = RtmpConstants.MSG_AUDIO,
            payload = byteArrayOf(1),
        )
        writeType0(
            input,
            chunkStreamId = 7,
            timestamp = 2000,
            typeId = RtmpConstants.MSG_VIDEO,
            payload = byteArrayOf(2),
        )
        writeType2(input, chunkStreamId = 6, timestampDelta = 20, payload = byteArrayOf(3))
        writeType1(
            input,
            chunkStreamId = 7,
            timestampDelta = 40,
            typeId = RtmpConstants.MSG_VIDEO,
            payload = byteArrayOf(4),
        )
        writeType3(input, chunkStreamId = 6, payload = byteArrayOf(5))
        writeType3(input, chunkStreamId = 7, payload = byteArrayOf(6))

        val messages = RtmpChunkCodec().readMessages(input)

        assertEquals(listOf(1000, 2000, 1020, 2040, 1040, 2080), messages.map(RtmpMessage::timestamp))
        assertEquals(
            listOf(
                RtmpConstants.MSG_AUDIO,
                RtmpConstants.MSG_VIDEO,
                RtmpConstants.MSG_AUDIO,
                RtmpConstants.MSG_VIDEO,
                RtmpConstants.MSG_AUDIO,
                RtmpConstants.MSG_VIDEO,
            ),
            messages.map(RtmpMessage::typeId),
        )
        input.release()
    }

    @Test
    fun `rejects messages whose declared length exceeds the limit`() {
        val payload = ByteArray(600) { index -> (index % 251).toByte() }
        val message = RtmpMessage(
            timestamp = 1,
            typeId = RtmpConstants.MSG_VIDEO,
            streamId = 1,
            payload = payload,
        )

        val encoded = RtmpChunkCodec().encode(message, chunkStreamId = 6)

        assertFailsWith<IllegalStateException> {
            RtmpChunkCodec(maxMessageBytes = 128).readMessages(encoded)
        }
    }

    @Test
    fun `roundtrips long message split into default chunks`() {
        val payload = ByteArray(600) { index -> (index % 251).toByte() }
        val message = RtmpMessage(
            timestamp = 42,
            typeId = RtmpConstants.MSG_VIDEO,
            streamId = 1,
            payload = payload,
        )

        val encoded = RtmpChunkCodec().encode(message, chunkStreamId = 6)
        val decoded = RtmpChunkCodec().readMessages(encoded)

        assertEquals(1, decoded.size)
        assertEquals(message.timestamp, decoded.single().timestamp)
        assertEquals(message.typeId, decoded.single().typeId)
        assertEquals(message.streamId, decoded.single().streamId)
        assertContentEquals(payload, decoded.single().payload)
    }

    @Test
    fun `waits for complete chunk before emitting a message`() {
        val message = RtmpClientRequests.connect("live", "rtmp://localhost/live")
        val encoded = RtmpChunkCodec().encode(message, chunkStreamId = 3)
        val firstHalf = encoded.readRetainedSlice(encoded.readableBytes() / 2)
        val secondHalf = encoded.readRetainedSlice(encoded.readableBytes())
        val input = Unpooled.buffer()
        val decoder = RtmpChunkCodec()

        input.writeBytes(firstHalf)
        assertEquals(emptyList(), decoder.readMessages(input))

        input.writeBytes(secondHalf)
        val decoded = decoder.readMessages(input)
        assertEquals(1, decoded.size)
        assertEquals("connect", decoder.command(decoded.single())?.name)

        firstHalf.release()
        secondHalf.release()
        input.release()
    }

    private fun writeType0(
        out: io.netty.buffer.ByteBuf,
        chunkStreamId: Int,
        timestamp: Int,
        messageLength: Int = 1,
        typeId: Int = RtmpConstants.MSG_AUDIO,
        payload: ByteArray,
    ) {
        writeBasicHeader(out, format = 0, chunkStreamId)
        out.writeMedium(if (timestamp >= MAX_BASIC_TIMESTAMP) MAX_BASIC_TIMESTAMP else timestamp)
        out.writeMedium(messageLength)
        out.writeByte(typeId)
        out.writeIntLE(1)
        if (timestamp >= MAX_BASIC_TIMESTAMP) out.writeInt(timestamp)
        out.writeBytes(payload)
    }

    private fun writeType1(
        out: io.netty.buffer.ByteBuf,
        chunkStreamId: Int,
        timestampDelta: Int,
        messageLength: Int = 1,
        typeId: Int = RtmpConstants.MSG_AUDIO,
        payload: ByteArray,
    ) {
        writeBasicHeader(out, format = 1, chunkStreamId)
        out.writeMedium(if (timestampDelta >= MAX_BASIC_TIMESTAMP) MAX_BASIC_TIMESTAMP else timestampDelta)
        out.writeMedium(messageLength)
        out.writeByte(typeId)
        if (timestampDelta >= MAX_BASIC_TIMESTAMP) out.writeInt(timestampDelta)
        out.writeBytes(payload)
    }

    private fun writeType2(
        out: io.netty.buffer.ByteBuf,
        chunkStreamId: Int,
        timestampDelta: Int,
        payload: ByteArray,
    ) {
        writeBasicHeader(out, format = 2, chunkStreamId)
        out.writeMedium(if (timestampDelta >= MAX_BASIC_TIMESTAMP) MAX_BASIC_TIMESTAMP else timestampDelta)
        if (timestampDelta >= MAX_BASIC_TIMESTAMP) out.writeInt(timestampDelta)
        out.writeBytes(payload)
    }

    private fun writeType3(
        out: io.netty.buffer.ByteBuf,
        chunkStreamId: Int,
        extendedTimestamp: Int? = null,
        payload: ByteArray,
    ) {
        writeBasicHeader(out, format = 3, chunkStreamId)
        extendedTimestamp?.let(out::writeInt)
        out.writeBytes(payload)
    }

    private fun writeBasicHeader(out: io.netty.buffer.ByteBuf, format: Int, chunkStreamId: Int) {
        out.writeByte((format shl 6) or chunkStreamId)
    }

    private companion object {
        const val MAX_BASIC_TIMESTAMP = 0x00ff_ffff
    }
}
