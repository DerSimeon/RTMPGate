package lol.simeon.rtmpgate.rtmp

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import kotlinx.coroutines.runBlocking
import lol.simeon.rtmpgate.config.AppConfig
import lol.simeon.rtmpgate.metrics.RtmpGateMetrics
import lol.simeon.rtmpgate.routes.InMemoryRouteStore
import lol.simeon.rtmpgate.routes.RouteRecord
import lol.simeon.rtmpgate.runtime.AppState
import lol.simeon.rtmpgate.testConfig
import org.slf4j.LoggerFactory
import java.util.UUID
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** End-to-end: test encoder -> RtmpRelayServer -> FakeRtmpUpstream over loopback sockets. */
class RtmpUpstreamReconnectTest {
    private val streamKey = "key-${UUID.randomUUID()}"
    private val registry = RtmpSessionRegistry()
    private val opened = mutableListOf<AutoCloseable>()
    private val logs = ListAppender<ILoggingEvent>()
    private val rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) as Logger

    private val baseConfig: AppConfig = testConfig().copy(
        publishConfirmTimeoutMillis = 1_000,
        upstreamRetryWindowMillis = 10_000,
        upstreamRetryBaseDelayMillis = 50,
        upstreamRetryMaxDelayMillis = 200,
    )

    @BeforeTest
    fun setUp() {
        logs.start()
        rootLogger.addAppender(logs)
    }

    @AfterTest
    fun tearDown() {
        opened.asReversed().forEach { runCatching { it.close() } }
        rootLogger.detachAppender(logs)
    }

    @Test
    fun `encoder stays connected through an upstream reset and resumes with replayed headers at a keyframe`() {
        val recoveredBefore = outcomes()["recovered"] ?: 0
        val upstream = upstream { index ->
            when (index) {
                0 -> PublishBehavior.AcceptThenReset(afterMillis = 300)
                1 -> PublishBehavior.RejectWithStatus("NetStream.Publish.BadName", GOOGLE_DUPLICATE_INPUT)
                else -> PublishBehavior.Accept
            }
        }
        val encoder = gateAndEncoder(baseConfig, upstream)

        assertEquals("NetStream.Publish.Start", encoder.publish().code)
        val sessionId = registry.list().single().id
        encoder.startStreaming()

        eventually(timeoutMillis = 10_000) {
            upstream.connections.size >= 3 && upstream.connections[2].media.any(::isKeyframe)
        }

        assertTrue(encoder.isOpen(), "encoder connection must survive the upstream outage")
        val session = registry.list().single()
        assertEquals(sessionId, session.id)
        assertEquals("relaying", session.state)
        assertTrue(upstream.connections[1].media.isEmpty(), "nothing is sent to a rejected upstream")

        // Replayed headers come first, restamped at one current timestamp...
        val resumed = upstream.connections[2].media.toList()
        val replayed = resumed.take(3)
        assertTrue(RtmpMediaInspector.isMetadata(replayed[0].toPacket()), "metadata first")
        assertTrue(RtmpMediaInspector.isVideoSequenceHeader(replayed[1].toPacket()), "then the AVC sequence header")
        assertTrue(RtmpMediaInspector.isAudioSequenceHeader(replayed[2].toPacket()), "then the AAC sequence header")
        assertEquals(1, replayed.map { it.timestamp }.distinct().size)
        assertTrue(replayed[0].timestamp > 0, "replayed headers carry the stream's current timestamp")
        // ...and video resumes at a keyframe, never with an inter frame.
        val firstVideo = resumed.drop(3).first { it.typeId == RtmpConstants.MSG_VIDEO }
        assertTrue(isKeyframe(firstVideo), "video must resume at a keyframe")

        assertEquals(1, logLines("Upstream relay lost").size)
        assertEquals(1, logLines("Upstream reconnect attempt failed").size)
        assertContainsLine("Upstream reconnect attempt failed", GOOGLE_DUPLICATE_INPUT)
        assertEquals(1, logLines("Upstream relay restored").size)
        assertEquals(recoveredBefore + 1, outcomes()["recovered"])
    }

    @Test
    fun `encoder is closed once the reconnect window runs out`() {
        val exhaustedBefore = outcomes()["exhausted"] ?: 0
        val upstream = upstream { index ->
            if (index == 0) {
                PublishBehavior.AcceptThenReset(afterMillis = 200)
            } else {
                PublishBehavior.RejectWithStatus("NetStream.Publish.BadName", GOOGLE_DUPLICATE_INPUT)
            }
        }
        val encoder = gateAndEncoder(baseConfig.copy(upstreamRetryWindowMillis = 1_500), upstream)

        assertEquals("NetStream.Publish.Start", encoder.publish().code)
        encoder.startStreaming()

        eventually(timeoutMillis = 8_000) { !encoder.isOpen() }
        eventually { registry.count() == 0 }
        assertTrue(upstream.connections.size >= 2, "at least one reconnect was attempted")
        assertEquals(1, logLines("upstream reconnect window exhausted").size)
        assertEquals(exhaustedBefore + 1, outcomes()["exhausted"])
    }

    @Test
    fun `with retries disabled an upstream failure closes the session and is logged once per batch`() {
        val upstream = upstream { PublishBehavior.AcceptThenReset(afterMillis = 100) }
        val encoder = gateAndEncoder(baseConfig.copy(upstreamRetryWindowMillis = 0), upstream)

        assertEquals("NetStream.Publish.Start", encoder.publish().code)
        encoder.send(TestMedia.metadata(0), TestMedia.avcSequenceHeader(0), TestMedia.aacSequenceHeader(0))
        eventually { upstream.connections.single().closed }
        Thread.sleep(300)

        // One inbound batch of many packets after the upstream died.
        encoder.send(*Array(30) { TestMedia.interFrame(40 + it * 40) })

        eventually { !encoder.isOpen() }
        assertEquals(1, logLines("Closing RTMP session: upstream writer failed").size)
        assertEquals(1, upstream.connections.size, "no reconnect when retries are disabled")
    }

    @Test
    fun `upstream rejecting the first publish is passed on to the encoder as an error status`() {
        val upstream = upstream { PublishBehavior.RejectWithStatus("NetStream.Publish.BadName", GOOGLE_DUPLICATE_INPUT) }
        val encoder = gateAndEncoder(baseConfig, upstream)

        val status = encoder.publish()

        assertTrue(status.isErrorStatus(), "expected an error status, got ${status.statusSummary()}")
        eventually { registry.count() == 0 }
        assertContainsLine("Failed to start upstream relay", GOOGLE_DUPLICATE_INPUT)
    }

    @Test
    fun `with confirmation disabled a silent upstream still relays`() {
        val upstream = upstream { PublishBehavior.Silent }
        val encoder = gateAndEncoder(baseConfig.copy(waitForPublishStart = false), upstream)

        assertEquals("NetStream.Publish.Start", encoder.publish().code)
        encoder.send(TestMedia.avcSequenceHeader(0), TestMedia.keyframe(0))

        eventually { upstream.connections.single().media.size >= 2 }
    }

    private fun upstream(behaviorFor: (Int) -> PublishBehavior): FakeRtmpUpstream =
        FakeRtmpUpstream(behaviorFor = behaviorFor).also { opened += it }

    private fun gateAndEncoder(config: AppConfig, upstream: FakeRtmpUpstream): TestRtmpEncoder {
        val routes = InMemoryRouteStore()
        runBlocking { routes.put(RouteRecord(streamKey, upstream.targetUrl(streamKey), 0, 0)) }
        val gate = RtmpRelayServer(config, routes, registry, AppState()).start().also { opened += it }
        return TestRtmpEncoder(gate.boundPort, streamKey).also { opened += it }
    }

    /** A coded keyframe; an AVC sequence header also carries frame type 1 but is not one. */
    private fun isKeyframe(message: RtmpMessage): Boolean {
        val packet = message.toPacket()
        return RtmpMediaInspector.isKeyframe(packet) && !RtmpMediaInspector.isVideoSequenceHeader(packet)
    }

    private fun outcomes(): Map<String, Long> = RtmpGateMetrics.snapshot().upstreamReconnectOutcomes

    /** Formatted log lines from this test's session that contain [text]. */
    private fun logLines(text: String): List<String> {
        val events = synchronized(logs) { logs.list.toList() }
        return events.map { it.formattedMessage }.filter { it.contains(text) && it.contains(streamKey) }
    }

    private fun assertContainsLine(text: String, detail: String) {
        assertTrue(logLines(text).any { it.contains(detail) }, "no '$text' log line containing '$detail'")
    }
}
