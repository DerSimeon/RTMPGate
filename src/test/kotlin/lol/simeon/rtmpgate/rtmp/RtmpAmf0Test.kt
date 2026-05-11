package lol.simeon.rtmpgate.rtmp

import kotlin.test.Test
import kotlin.test.assertEquals

class RtmpAmf0Test {
    @Test
    fun `parses connect command app`() {
        val command = RtmpAmf0.parseCommand(RtmpClientRequests.connect("live", "rtmp://localhost/live"))

        assertEquals("connect", command?.name)
        assertEquals(1.0, command?.transactionId)
        assertEquals("live", command?.app)
    }

    @Test
    fun `parses publish command stream key`() {
        val command = RtmpAmf0.parseCommand(RtmpClientRequests.publish(1, "test-key"))

        assertEquals("publish", command?.name)
        assertEquals("test-key", command?.streamKey)
    }

    @Test
    fun `parses release stream command stream key`() {
        val command = RtmpAmf0.parseCommand(RtmpClientRequests.releaseStream("abc"))

        assertEquals("releaseStream", command?.name)
        assertEquals(2.0, command?.transactionId)
        assertEquals("abc", command?.streamKey)
    }
}
