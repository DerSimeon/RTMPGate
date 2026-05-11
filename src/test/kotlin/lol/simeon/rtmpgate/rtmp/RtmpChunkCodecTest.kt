package lol.simeon.rtmpgate.rtmp

import io.netty.buffer.Unpooled
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class RtmpChunkCodecTest {
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
}
