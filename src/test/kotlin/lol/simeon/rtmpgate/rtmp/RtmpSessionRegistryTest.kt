package lol.simeon.rtmpgate.rtmp

import io.netty.channel.embedded.EmbeddedChannel
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RtmpSessionRegistryTest {
    @Test
    fun `registers lists updates and closes sessions`() {
        val registry = RtmpSessionRegistry()
        val channel = EmbeddedChannel()
        val session = registry.register(channel)

        assertEquals(1, registry.count())
        registry.updatePublish(session.id, "key", "rtmp://localhost/live/target", "relaying")

        val listed = registry.list().single()
        assertEquals("key", listed.streamKey)
        assertEquals("relaying", listed.state)

        assertTrue(registry.close(session.id))
        registry.unregister(session.id)
        assertEquals(0, registry.count())
    }
}
