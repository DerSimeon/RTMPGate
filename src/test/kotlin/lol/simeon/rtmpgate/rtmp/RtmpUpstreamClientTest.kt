package lol.simeon.rtmpgate.rtmp

import lol.simeon.rtmpgate.config.AppConfig
import lol.simeon.rtmpgate.testConfig
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RtmpUpstreamClientTest {
    private val config = testConfig().copy(publishConfirmTimeoutMillis = 1_000)
    private val opened = mutableListOf<AutoCloseable>()

    @AfterTest
    fun tearDown() {
        opened.asReversed().forEach { runCatching { it.close() } }
    }

    @Test
    fun `publish succeeds once upstream confirms NetStream Publish Start`() {
        val upstream = upstream { PublishBehavior.Accept }
        val client = client(upstream)

        client.connectAndPublish()

        assertNull(client.failure())
        val packet = TestMedia.keyframe(0).toPacket()
        try {
            assertTrue(client.writeMedia(packet))
        } finally {
            packet.release()
        }
    }

    @Test
    fun `error onStatus after publish fails with the upstream code and description`() {
        val upstream = upstream { PublishBehavior.RejectWithStatus("NetStream.Publish.BadName", GOOGLE_DUPLICATE_INPUT) }

        val error = assertFailsWith<RtmpUpstreamException> { client(upstream).connectAndPublish() }

        assertContains(error.message.orEmpty(), "NetStream.Publish.BadName")
        assertContains(error.message.orEmpty(), GOOGLE_DUPLICATE_INPUT)
    }

    @Test
    fun `_error reply to publish fails the publish`() {
        val upstream = upstream { PublishBehavior.ErrorCommand }

        val error = assertFailsWith<RtmpUpstreamException> { client(upstream).connectAndPublish() }

        assertContains(error.message.orEmpty(), "_error")
        assertContains(error.message.orEmpty(), "NetStream.Publish.Denied")
    }

    @Test
    fun `upstream closing after publish fails the publish`() {
        val upstream = upstream { PublishBehavior.CloseAfterPublish }

        val error = assertFailsWith<RtmpUpstreamException> { client(upstream).connectAndPublish() }

        assertContains(error.message.orEmpty(), "closed", ignoreCase = true)
    }

    @Test
    fun `silent upstream times out on the publish confirm timeout`() {
        val upstream = upstream { PublishBehavior.Silent }
        val started = System.currentTimeMillis()

        val error = assertFailsWith<RtmpUpstreamException> {
            client(upstream, config.copy(publishConfirmTimeoutMillis = 300)).connectAndPublish()
        }

        assertContains(error.message.orEmpty(), "Timed out after 300ms")
        assertTrue(System.currentTimeMillis() - started < config.readTimeoutMillis, "must not wait for the read timeout")
    }

    @Test
    fun `confirmation switch off restores send-and-assume publish`() {
        val upstream = upstream { PublishBehavior.Silent }

        client(upstream, config.copy(waitForPublishStart = false)).connectAndPublish()
    }

    @Test
    fun `error onStatus while waiting for connect result is a failure, not a reply`() {
        val upstream = upstream(rejectConnect = true) { PublishBehavior.Accept }

        val error = assertFailsWith<RtmpUpstreamException> { client(upstream).connectAndPublish() }

        assertContains(error.message.orEmpty(), "NetConnection.Connect.Rejected")
    }

    @Test
    fun `_error for releaseStream is ignored`() {
        val upstream = upstream(releaseStreamError = true) { PublishBehavior.Accept }

        client(upstream).connectAndPublish()
    }

    @Test
    fun `upstream reset after a confirmed publish is reported through failure`() {
        val upstream = upstream { PublishBehavior.AcceptThenReset(afterMillis = 100) }
        val client = client(upstream)

        client.connectAndPublish()

        eventually(timeoutMillis = 3_000) { client.failure() != null }
        assertNotNull(client.failure())
        val packet = TestMedia.keyframe(0).toPacket()
        try {
            assertTrue(!client.writeMedia(packet), "writes after a failure are refused")
        } finally {
            packet.release()
        }
    }

    private fun upstream(
        rejectConnect: Boolean = false,
        releaseStreamError: Boolean = false,
        behaviorFor: (Int) -> PublishBehavior,
    ): FakeRtmpUpstream = FakeRtmpUpstream(rejectConnect, releaseStreamError, behaviorFor).also { opened += it }

    private fun client(upstream: FakeRtmpUpstream, clientConfig: AppConfig = config): RtmpUpstreamClient =
        RtmpUpstreamClient(upstream.targetUrl("key"), "live", "key", clientConfig).also { opened += it }
}

fun eventually(timeoutMillis: Long = 5_000, condition: () -> Boolean) {
    val deadline = System.currentTimeMillis() + timeoutMillis
    while (System.currentTimeMillis() < deadline) {
        if (condition()) return
        Thread.sleep(20)
    }
    check(condition()) { "Condition not met within ${timeoutMillis}ms" }
}
