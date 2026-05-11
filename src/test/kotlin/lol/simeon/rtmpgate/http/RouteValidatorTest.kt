package lol.simeon.rtmpgate.http

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RouteValidatorTest {
    @Test
    fun `accepts normal stream keys`() {
        assertEquals("abc-123_live.key", RouteValidator.requireValidStreamKey("abc-123_live.key"))
        assertEquals("abc:123@test", RouteValidator.requireValidStreamKey("abc:123@test"))
    }

    @Test
    fun `rejects unsafe stream keys`() {
        assertFailsWith<IllegalArgumentException> { RouteValidator.requireValidStreamKey("") }
        assertFailsWith<IllegalArgumentException> { RouteValidator.requireValidStreamKey("../secret") }
        assertFailsWith<IllegalArgumentException> { RouteValidator.requireValidStreamKey("key/with/slash") }
        assertFailsWith<IllegalArgumentException> { RouteValidator.requireValidStreamKey(" key with spaces ") }
        assertFailsWith<IllegalArgumentException> { RouteValidator.requireValidStreamKey("a".repeat(129)) }
    }

    @Test
    fun `accepts rtmp targets`() {
        assertEquals(
            "rtmp://127.0.0.1:1935/live/target",
            RouteValidator.requireValidTarget("rtmp://127.0.0.1:1935/live/target"),
        )
    }

    @Test
    fun `supports target host allowlist`() {
        assertEquals(
            "rtmp://media.example.com/live/target",
            RouteValidator.requireValidTarget("rtmp://media.example.com/live/target", setOf("media.example.com")),
        )
        assertFailsWith<IllegalArgumentException> {
            RouteValidator.requireValidTarget("rtmp://evil.example.com/live/target", setOf("media.example.com"))
        }
    }

    @Test
    fun `rejects non rtmp targets`() {
        assertFailsWith<IllegalArgumentException> { RouteValidator.requireValidTarget("http://localhost/live/key") }
        assertFailsWith<IllegalArgumentException> { RouteValidator.requireValidTarget("rtmp:///live/key") }
        assertFailsWith<IllegalArgumentException> { RouteValidator.requireValidTarget("rtmp://localhost") }
        assertFailsWith<IllegalArgumentException> { RouteValidator.requireValidTarget("rtmp://localhost/live/key?x=1") }
    }
}
