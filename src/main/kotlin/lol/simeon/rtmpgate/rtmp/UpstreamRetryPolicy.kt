package lol.simeon.rtmpgate.rtmp

import kotlin.random.Random

/**
 * Exponential backoff with "equal jitter" for upstream reconnect attempts: the nominal delay
 * doubles per attempt from [baseDelayMillis] up to [maxDelayMillis], and the actual delay is a
 * random value between half the nominal delay and the full nominal delay. The jitter keeps
 * many sessions that lost the same upstream at the same moment from reconnecting in lockstep.
 */
class UpstreamRetryPolicy(
    baseDelayMillis: Long,
    maxDelayMillis: Long,
    private val random: Random = Random.Default,
) {
    private val baseDelayMillis = baseDelayMillis.coerceAtLeast(1)
    private val maxDelayMillis = maxDelayMillis.coerceAtLeast(this.baseDelayMillis)

    /** Delay before the 1-based [attempt]. */
    fun delayBeforeAttempt(attempt: Int): Long {
        val exponent = (attempt - 1).coerceIn(0, MAX_EXPONENT)
        val nominal = (baseDelayMillis shl exponent).coerceIn(baseDelayMillis, maxDelayMillis)
        val half = nominal / 2
        return half + random.nextLong(nominal - half + 1)
    }

    private companion object {
        // 2^30 times any sane base delay already exceeds every sane max delay; also avoids overflow.
        const val MAX_EXPONENT = 30
    }
}
