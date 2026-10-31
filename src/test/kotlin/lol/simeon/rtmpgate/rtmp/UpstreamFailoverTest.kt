package lol.simeon.rtmpgate.rtmp

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class UpstreamRetryPolicyTest {
    @Test
    fun `delay grows exponentially with jitter and is capped`() {
        val policy = UpstreamRetryPolicy(baseDelayMillis = 1_000, maxDelayMillis = 15_000, random = Random(42))

        repeat(200) {
            assertTrue(policy.delayBeforeAttempt(1) in 500..1_000)
            assertTrue(policy.delayBeforeAttempt(2) in 1_000..2_000)
            assertTrue(policy.delayBeforeAttempt(3) in 2_000..4_000)
            assertTrue(policy.delayBeforeAttempt(10) in 7_500..15_000)
            assertTrue(policy.delayBeforeAttempt(10_000) in 7_500..15_000)
        }
    }

    @Test
    fun `jitter spreads the delays`() {
        val policy = UpstreamRetryPolicy(baseDelayMillis = 1_000, maxDelayMillis = 15_000, random = Random(7))

        assertTrue((1..50).map { policy.delayBeforeAttempt(4) }.distinct().size > 10)
    }
}

class UpstreamGapTrackerTest {
    @Test
    fun `a failure soon after a reconnect continues the episode`() {
        val tracker = UpstreamGapTracker(stableRelayMillis = 30_000)

        assertFalse(tracker.startGap(now = 1_000))
        tracker.recovered(now = 5_000, attempts = 3)
        assertTrue(tracker.startGap(now = 8_000), "failed 3 s after the reconnect")

        assertEquals(1_000, tracker.episodeStartedAt)
        assertEquals(3, tracker.attempts)
        assertEquals(7_000, tracker.episodeMillis(now = 8_000))
    }

    @Test
    fun `a failure after a stable relay starts a new episode`() {
        val tracker = UpstreamGapTracker(stableRelayMillis = 30_000)

        tracker.startGap(now = 1_000)
        tracker.recovered(now = 5_000, attempts = 3)
        assertFalse(tracker.startGap(now = 60_000))

        assertEquals(60_000, tracker.episodeStartedAt)
        assertEquals(0, tracker.attempts)
    }

    @Test
    fun `endGap reports the gap once`() {
        val tracker = UpstreamGapTracker()

        tracker.startGap(now = 1_000)

        assertEquals(2_500, tracker.endGap("encoder_closed", now = 3_500))
        assertNull(tracker.endGap("encoder_closed", now = 4_000))
    }
}

class RtmpRelayResumeTest {
    @Test
    fun `replays latest metadata and sequence headers at the current timestamp`() {
        val resume = RtmpRelayResume()
        observe(resume, TestMedia.metadata(0), TestMedia.avcSequenceHeader(0), TestMedia.aacSequenceHeader(0))
        observe(resume, TestMedia.keyframe(40), TestMedia.aacFrame(60))
        // A new AVC configuration mid-stream replaces the cached one.
        val newAvcConfig = TestMedia.avcSequenceHeader(80).copy(payload = byteArrayOf(0x17, 0x00, 0x09, 0x09))
        observe(resume, newAvcConfig, TestMedia.interFrame(120))

        val replay = resume.beginResume()
        try {
            assertEquals(3, replay.size)
            assertTrue(RtmpMediaInspector.isMetadata(replay[0]))
            assertEquals(newAvcConfig.payload.toList(), replay[1].toMessage().payload.toList())
            assertTrue(RtmpMediaInspector.isAudioSequenceHeader(replay[2]))
            assertTrue(replay.all { it.timestamp == 120 })
        } finally {
            replay.forEach(RtmpPacket::release)
            resume.release()
        }
    }

    @Test
    fun `holds video until the next keyframe after a resume`() {
        val resume = RtmpRelayResume()
        resume.beginResume()

        assertFalse(admit(resume, TestMedia.interFrame(0)))
        assertTrue(admit(resume, TestMedia.aacFrame(0)), "audio is not held")
        assertTrue(admit(resume, TestMedia.avcSequenceHeader(0)), "sequence headers are not held")
        assertTrue(admit(resume, TestMedia.keyframe(40)))
        assertTrue(admit(resume, TestMedia.interFrame(80)), "inter frames pass after the keyframe")
        resume.release()
    }

    @Test
    fun `nothing is held before any resume`() {
        val resume = RtmpRelayResume()

        assertTrue(admit(resume, TestMedia.interFrame(0)))
        assertTrue(resume.beginResume().isEmpty(), "nothing cached yet")
    }

    @Test
    fun `recognises Enhanced RTMP sequence start and keyframes`() {
        // IsExHeader | FrameType=1 (key) | PacketType=0 (SequenceStart), FourCC "hvc1".
        val sequenceStart = RtmpMessage(0, RtmpConstants.MSG_VIDEO, 1, byteArrayOf(0x90.toByte(), 0x68, 0x76, 0x63, 0x31))
        // IsExHeader | FrameType=1 | PacketType=1 (CodedFrames).
        val keyframe = RtmpMessage(0, RtmpConstants.MSG_VIDEO, 1, byteArrayOf(0x91.toByte(), 0x68, 0x76, 0x63, 0x31))

        assertTrue(RtmpMediaInspector.isVideoSequenceHeader(sequenceStart.toPacket()))
        assertTrue(RtmpMediaInspector.isKeyframe(keyframe.toPacket()))
        assertFalse(RtmpMediaInspector.isVideoSequenceHeader(keyframe.toPacket()))
    }

    private fun observe(resume: RtmpRelayResume, vararg messages: RtmpMessage) {
        messages.forEach { message ->
            val packet = message.toPacket()
            resume.observe(packet)
            packet.release()
        }
    }

    private fun admit(resume: RtmpRelayResume, message: RtmpMessage): Boolean = resume.admit(message.toPacket())
}
