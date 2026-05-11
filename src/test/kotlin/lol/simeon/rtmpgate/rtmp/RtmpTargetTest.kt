package lol.simeon.rtmpgate.rtmp

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RtmpTargetTest {
    @Test
    fun `parses rtmp target`() {
        val target = RtmpTarget.parse("rtmp://example.com:1936/live/some-id", "fallback", "fallback-key")

        assertEquals("example.com", target.host)
        assertEquals(1936, target.port)
        assertEquals("live", target.app)
        assertEquals("some-id", target.streamKey)
        assertEquals("rtmp://example.com:1936/live", target.tcUrl)
    }

    @Test
    fun `uses default port and fallback path parts`() {
        val target = RtmpTarget.parse("rtmp://example.com/live", "fallback", "fallback-key")

        assertEquals(1935, target.port)
        assertEquals("live", target.app)
        assertEquals("fallback-key", target.streamKey)
    }

    @Test
    fun `rejects non rtmp target`() {
        assertFailsWith<IllegalArgumentException> {
            RtmpTarget.parse("http://example.com/live/key", "live", "key")
        }
    }
}
