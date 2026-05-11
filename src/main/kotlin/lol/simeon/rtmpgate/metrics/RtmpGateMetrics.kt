package lol.simeon.rtmpgate.metrics

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

object RtmpGateMetrics {
    private val sessionsAccepted = AtomicLong()
    private val sessionsClosed = AtomicLong()
    private val sessionsRejected = AtomicLong()
    private val publishAttempts = AtomicLong()
    private val publishAccepted = AtomicLong()
    private val publishRejected = AtomicLong()
    private val upstreamFailures = AtomicLong()
    private val activeSessions = AtomicLong()
    private val activeRelays = AtomicLong()
    private val bytesIn = AtomicLong()
    private val bytesOut = AtomicLong()
    private val routeLookupNanos = AtomicLong()
    private val routeLookupCount = AtomicLong()
    private val closeReasons = ConcurrentHashMap<String, AtomicLong>()

    fun sessionAccepted() {
        sessionsAccepted.incrementAndGet()
        activeSessions.incrementAndGet()
    }

    fun sessionRejected(reason: String) {
        sessionsRejected.incrementAndGet()
        closeReason(reason)
    }

    fun sessionClosed(reason: String = "unknown") {
        sessionsClosed.incrementAndGet()
        activeSessions.updateAndGet { value -> (value - 1).coerceAtLeast(0) }
        closeReason(reason)
    }

    fun publishAttempt() {
        publishAttempts.incrementAndGet()
    }

    fun publishAccepted() {
        publishAccepted.incrementAndGet()
        activeRelays.incrementAndGet()
    }

    fun relayClosed() {
        activeRelays.updateAndGet { value -> (value - 1).coerceAtLeast(0) }
    }

    fun publishRejected(reason: String = "unknown") {
        publishRejected.incrementAndGet()
        closeReason(reason)
    }

    fun upstreamFailure() {
        upstreamFailures.incrementAndGet()
    }

    fun bytesIn(bytes: Long) {
        bytesIn.addAndGet(bytes.coerceAtLeast(0))
    }

    fun bytesOut(bytes: Long) {
        bytesOut.addAndGet(bytes.coerceAtLeast(0))
    }

    fun routeLookup(durationNanos: Long) {
        routeLookupNanos.addAndGet(durationNanos.coerceAtLeast(0))
        routeLookupCount.incrementAndGet()
    }

    private fun closeReason(reason: String) {
        closeReasons.computeIfAbsent(reason.sanitizeMetricLabel()) { AtomicLong() }.incrementAndGet()
    }

    fun snapshot(): MetricsSnapshot = MetricsSnapshot(
        sessionsAccepted = sessionsAccepted.get(),
        sessionsClosed = sessionsClosed.get(),
        sessionsRejected = sessionsRejected.get(),
        publishAttempts = publishAttempts.get(),
        publishAccepted = publishAccepted.get(),
        publishRejected = publishRejected.get(),
        upstreamFailures = upstreamFailures.get(),
        activeSessions = activeSessions.get(),
        activeRelays = activeRelays.get(),
        bytesIn = bytesIn.get(),
        bytesOut = bytesOut.get(),
        routeLookupCount = routeLookupCount.get(),
        routeLookupNanos = routeLookupNanos.get(),
        closeReasons = closeReasons.mapValues { it.value.get() },
    )

    fun prometheus(): String {
        val s = snapshot()
        return buildString {
            appendCounter("rtmpgate_sessions_accepted_total", s.sessionsAccepted)
            appendCounter("rtmpgate_sessions_closed_total", s.sessionsClosed)
            appendCounter("rtmpgate_sessions_rejected_total", s.sessionsRejected)
            appendCounter("rtmpgate_publish_attempts_total", s.publishAttempts)
            appendCounter("rtmpgate_publish_accepted_total", s.publishAccepted)
            appendCounter("rtmpgate_publish_rejected_total", s.publishRejected)
            appendCounter("rtmpgate_upstream_failures_total", s.upstreamFailures)
            appendCounter("rtmpgate_bytes_in_total", s.bytesIn)
            appendCounter("rtmpgate_bytes_out_total", s.bytesOut)
            appendCounter("rtmpgate_route_lookup_count", s.routeLookupCount)
            appendCounter("rtmpgate_route_lookup_duration_seconds_total", s.routeLookupNanos / 1_000_000_000.0)
            appendGauge("rtmpgate_active_sessions", s.activeSessions)
            appendGauge("rtmpgate_active_relays", s.activeRelays)
            s.closeReasons.forEach { (reason, value) ->
                appendLine("# TYPE rtmpgate_close_reasons_total counter")
                appendLine("rtmpgate_close_reasons_total{reason=\"$reason\"} $value")
            }
        }
    }

    private fun StringBuilder.appendCounter(name: String, value: Number) {
        appendLine("# TYPE $name counter")
        appendLine("$name $value")
    }

    private fun StringBuilder.appendGauge(name: String, value: Number) {
        appendLine("# TYPE $name gauge")
        appendLine("$name $value")
    }

    private fun String.sanitizeMetricLabel(): String = replace(Regex("[^A-Za-z0-9_:-]"), "_")
}

data class MetricsSnapshot(
    val sessionsAccepted: Long,
    val sessionsClosed: Long,
    val sessionsRejected: Long,
    val publishAttempts: Long,
    val publishAccepted: Long,
    val publishRejected: Long,
    val upstreamFailures: Long,
    val activeSessions: Long,
    val activeRelays: Long,
    val bytesIn: Long,
    val bytesOut: Long,
    val routeLookupCount: Long,
    val routeLookupNanos: Long,
    val closeReasons: Map<String, Long>,
)
