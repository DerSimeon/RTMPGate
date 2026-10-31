package lol.simeon.rtmpgate.config

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AppConfigTest {
    @Test
    fun `upstream publish confirmation and reconnect defaults`() {
        val config = AppConfig.fromEnvironment(emptyMap())

        assertTrue(config.waitForPublishStart)
        assertEquals(10_000, config.publishConfirmTimeoutMillis)
        assertEquals(600_000, config.upstreamRetryWindowMillis)
        assertEquals(1_000, config.upstreamRetryBaseDelayMillis)
        assertEquals(15_000, config.upstreamRetryMaxDelayMillis)
    }

    @Test
    fun `upstream publish confirmation and reconnect overrides`() {
        val config = AppConfig.fromEnvironment(
            mapOf(
                "RTMPGATE_WAIT_FOR_PUBLISH_START" to "false",
                "RTMPGATE_PUBLISH_CONFIRM_TIMEOUT_MS" to "2500",
                "RTMPGATE_UPSTREAM_RETRY_WINDOW_MS" to "0",
                "RTMPGATE_UPSTREAM_RETRY_BASE_DELAY_MS" to "250",
                "RTMPGATE_UPSTREAM_RETRY_MAX_DELAY_MS" to "5000",
            ),
        )

        assertFalse(config.waitForPublishStart)
        assertEquals(2_500, config.publishConfirmTimeoutMillis)
        assertEquals(0, config.upstreamRetryWindowMillis)
        assertEquals(250, config.upstreamRetryBaseDelayMillis)
        assertEquals(5_000, config.upstreamRetryMaxDelayMillis)
    }

    @Test
    fun `unrecognised publish confirmation value keeps the default`() {
        val config = AppConfig.fromEnvironment(mapOf("RTMPGATE_WAIT_FOR_PUBLISH_START" to "maybe"))

        assertTrue(config.waitForPublishStart)
    }
}
