package lol.simeon.rtmpgate.rtmp

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

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

    @Test
    fun `parses onStatus info object`() {
        val command = RtmpAmf0.parseCommand(status("onStatus", "error", "NetStream.Publish.BadName", "Already publishing"))

        assertEquals("onStatus", command?.name)
        assertEquals("error", command?.level)
        assertEquals("NetStream.Publish.BadName", command?.code)
        assertEquals("Already publishing", command?.description)
        assertTrue(command!!.isErrorStatus())
    }

    @Test
    fun `status level onStatus is not an error`() {
        val command = RtmpAmf0.parseCommand(status("onStatus", "status", "NetStream.Publish.Start", "Start publishing."))!!

        assertFalse(command.isErrorStatus())
    }

    @Test
    fun `error codes count as errors even without level error`() {
        val command = RtmpAmf0.parseCommand(status("onStatus", "status", "NetStream.Publish.Failed", "nope"))!!

        assertTrue(command.isErrorStatus())
    }

    @Test
    fun `_error is always an error`() {
        val command = RtmpAmf0.parseCommand(status("_error", "error", "NetConnection.Call.Failed", "bad call"))!!

        assertEquals("NetConnection.Call.Failed", command.code)
        assertTrue(command.isErrorStatus())
    }

    private fun status(name: String, level: String, code: String, description: String): RtmpMessage {
        val payload = RtmpAmf0.concat(
            RtmpAmf0.string(name),
            RtmpAmf0.number(0.0),
            RtmpAmf0.nullValue(),
            RtmpAmf0.obj(
                "level" to RtmpAmf0.string(level),
                "code" to RtmpAmf0.string(code),
                "description" to RtmpAmf0.string(description),
                "clientid" to RtmpAmf0.number(7.0),
            ),
        )
        return RtmpMessage(0, RtmpConstants.MSG_COMMAND_AMF0, 1, payload)
    }
}
