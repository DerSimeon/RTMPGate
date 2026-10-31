package lol.simeon.rtmpgate.rtmp

import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import lol.simeon.rtmpgate.config.AppConfig
import lol.simeon.rtmpgate.metrics.RtmpGateMetrics
import org.slf4j.LoggerFactory

sealed interface UpstreamReconnectResult {
    /** A new upstream publish was confirmed. The receiver owns [client]. */
    data class Reconnected(val client: RtmpUpstreamClient, val attempts: Int) : UpstreamReconnectResult

    /** The episode's window ran out. */
    data class Exhausted(val attempts: Int, val lastReason: String?) : UpstreamReconnectResult
}

/** One reconnect run: who it is for and where its episode stands. Immutable, so it can cross threads. */
data class UpstreamReconnectRequest(
    val sessionId: String?,
    val info: RtmpPublishInfo,
    val target: String,
    val remoteAddress: String,
    val episodeStartedAt: Long,
    val attemptsSoFar: Int,
)

/**
 * Upstream failover for one relaying session: gap/episode bookkeeping, the cached headers and
 * keyframe gate for resuming ([relayResume]), the background reconnect loop and its log lines.
 *
 * Threading: [reconnect] runs on a background coroutine and touches only its request and the
 * volatile in-flight client. Every other method is confined to the session's event loop, and
 * the session decides all state transitions.
 */
class UpstreamFailover(
    private val config: AppConfig,
    private val retryPolicy: UpstreamRetryPolicy =
        UpstreamRetryPolicy(config.upstreamRetryBaseDelayMillis, config.upstreamRetryMaxDelayMillis),
) {
    private val logger = LoggerFactory.getLogger(UpstreamFailover::class.java)
    private val gaps = UpstreamGapTracker()
    private var current: UpstreamReconnectRequest? = null
    val relayResume = RtmpRelayResume()

    @Volatile
    private var connectingClient: RtmpUpstreamClient? = null

    /** Event loop. Opens a gap, logs the failure once, and returns the request for [reconnect]. */
    fun begin(
        sessionId: String?,
        info: RtmpPublishInfo,
        target: String,
        remoteAddress: String,
        reason: String,
    ): UpstreamReconnectRequest {
        val now = System.currentTimeMillis()
        val continuesEpisode = gaps.startGap(now)
        val request = UpstreamReconnectRequest(
            sessionId = sessionId,
            info = info,
            target = target,
            remoteAddress = remoteAddress,
            episodeStartedAt = gaps.episodeStartedAt ?: now,
            attemptsSoFar = gaps.attempts,
        )
        current = request
        logger.warn(
            "Upstream relay lost, keeping encoder connected and reconnecting sessionId={} streamKey={} target={} remote={} " +
                "continuesEpisode={} episodeElapsedMs={} retryWindowMs={} reason={}",
            sessionId,
            info.streamKey,
            target,
            remoteAddress,
            continuesEpisode,
            gaps.episodeMillis(now),
            config.upstreamRetryWindowMillis,
            reason,
        )
        return request
    }

    /**
     * Background. Retries with jittered exponential backoff until a publish is confirmed or the
     * episode's window runs out. Returns null when the coroutine was cancelled (session closed).
     */
    suspend fun reconnect(request: UpstreamReconnectRequest): UpstreamReconnectResult? {
        val deadline = request.episodeStartedAt + config.upstreamRetryWindowMillis
        var attempt = request.attemptsSoFar
        var lastReason: String? = null

        while (System.currentTimeMillis() < deadline) {
            val remaining = deadline - System.currentTimeMillis()
            delay(retryPolicy.delayBeforeAttempt(attempt + 1).coerceAtMost(remaining))
            if (System.currentTimeMillis() >= deadline) break

            attempt++
            RtmpGateMetrics.upstreamReconnectAttempt()
            val client = RtmpUpstreamClient(request.target, request.info.app, request.info.streamKey, config)
            val failure = connect(client) ?: return UpstreamReconnectResult.Reconnected(client, attempt)

            client.close()
            // The session closed (and aborted this connect) while we were connecting.
            if (!currentCoroutineContext().isActive) return null
            RtmpGateMetrics.upstreamFailure()
            lastReason = failure.message ?: failure::class.simpleName
            logger.warn(
                "Upstream reconnect attempt failed sessionId={} streamKey={} target={} attempt={} episodeElapsedMs={} " +
                    "errorType={} reason={}",
                request.sessionId,
                request.info.streamKey,
                request.target,
                attempt,
                System.currentTimeMillis() - request.episodeStartedAt,
                failure::class.simpleName,
                lastReason,
            )
        }

        return UpstreamReconnectResult.Exhausted(attempt, lastReason)
    }

    /**
     * Event loop. The new upstream has never seen this stream's metadata or decoder config:
     * send them first, stamped at the stream's current time; video then resumes at the next
     * keyframe through [relayResume].
     */
    fun recovered(client: RtmpUpstreamClient, attempts: Int) {
        val now = System.currentTimeMillis()
        val gapMillis = gaps.gapMillis(now)
        val replay = relayResume.beginResume()
        replay.forEach { packet ->
            try {
                client.writeMedia(packet)
            } finally {
                packet.release()
            }
        }
        gaps.recovered(now, attempts)
        logger.info(
            "Upstream relay restored sessionId={} streamKey={} target={} remote={} attempts={} gapMs={} episodeElapsedMs={} " +
                "replayedHeaders={}",
            current?.sessionId,
            current?.info?.streamKey,
            current?.target,
            current?.remoteAddress,
            attempts,
            gapMillis,
            gaps.episodeMillis(now),
            replay.size,
        )
    }

    /** Event loop. The window ran out; the session closes the encoder after this. */
    fun exhausted(attempts: Int, lastReason: String?) {
        val now = System.currentTimeMillis()
        logger.warn(
            "Closing RTMP session: upstream reconnect window exhausted sessionId={} streamKey={} target={} remote={} " +
                "attempts={} gapMs={} episodeElapsedMs={} lastReason={}",
            current?.sessionId,
            current?.info?.streamKey,
            current?.target,
            current?.remoteAddress,
            attempts,
            gaps.gapMillis(now),
            gaps.episodeMillis(now),
            lastReason,
        )
        gaps.endGap("exhausted", now)
    }

    /** Event loop, from channelInactive. Ends an open gap, aborts an in-flight connect, frees the cache. */
    fun sessionClosed() {
        connectingClient?.close()
        gaps.endGap("encoder_closed", System.currentTimeMillis())?.let { gapMillis ->
            logger.info(
                "Upstream reconnect stopped because the encoder session closed sessionId={} streamKey={} target={} gapMs={}",
                current?.sessionId,
                current?.info?.streamKey,
                current?.target,
                gapMillis,
            )
        }
        relayResume.release()
    }

    /** Blocking connect + confirmed publish. Returns the failure, or null on success. */
    private fun connect(client: RtmpUpstreamClient): Throwable? {
        connectingClient = client
        return try {
            client.connectAndPublish()
            null
        } catch (error: Throwable) {
            error
        } finally {
            connectingClient = null
        }
    }
}

/**
 * Bookkeeping for one session's upstream outages, confined to the session's event loop.
 * An episode is one reconnect window; a gap is one stretch without a working upstream. A relay
 * that fails again within [stableRelayMillis] of a reconnect (e.g. the upstream accepted the
 * publish and reset it seconds later) continues the episode, so the window and backoff are not
 * restarted by every short-lived success.
 */
class UpstreamGapTracker(private val stableRelayMillis: Long = DEFAULT_STABLE_RELAY_MILLIS) {
    var episodeStartedAt: Long? = null
        private set
    var attempts = 0
        private set
    private var gapStartedAt: Long? = null
    private var restoredAt: Long? = null

    /** Opens a gap. Returns true when it continues the previous episode. */
    fun startGap(now: Long): Boolean {
        val restored = restoredAt
        val continues = episodeStartedAt != null && restored != null && now - restored < stableRelayMillis
        if (!continues) {
            episodeStartedAt = now
            attempts = 0
            RtmpGateMetrics.upstreamRetryEpisodeStarted()
        }
        restoredAt = null
        gapStartedAt = now
        RtmpGateMetrics.upstreamGapStarted()
        return continues
    }

    fun gapMillis(now: Long): Long = now - (gapStartedAt ?: now)

    fun episodeMillis(now: Long): Long = now - (episodeStartedAt ?: now)

    fun recovered(now: Long, attempts: Int) {
        this.attempts = attempts
        restoredAt = now
        endGap("recovered", now)
    }

    /** Closes the open gap, if any, and returns its duration. [outcome] labels the metric. */
    fun endGap(outcome: String, now: Long): Long? {
        val startedAt = gapStartedAt ?: return null
        gapStartedAt = null
        val gapMillis = now - startedAt
        RtmpGateMetrics.upstreamGapEnded(outcome, gapMillis)
        return gapMillis
    }

    companion object {
        const val DEFAULT_STABLE_RELAY_MILLIS = 30_000L
    }
}
