package lol.simeon.rtmpgate.runtime

import java.util.concurrent.atomic.AtomicBoolean
import lol.simeon.rtmpgate.metrics.RtmpGateMetrics

class AppState {
    private val shuttingDown = AtomicBoolean(false)
    private val draining = AtomicBoolean(false)

    /**
     * Stops this instance from accepting new traffic without terminating established sessions.
     * The transition is intentionally one-way: a drained pod must be replaced rather than put
     * back into service.
     *
     * @return true when this call started draining, false when the instance was already draining.
     */
    fun beginDrain(): Boolean {
        val started = draining.compareAndSet(false, true)
        if (started) RtmpGateMetrics.drainStarted()
        return started
    }

    fun beginShutdown() {
        beginDrain()
        shuttingDown.set(true)
    }

    fun isShuttingDown(): Boolean = shuttingDown.get()

    fun isDraining(): Boolean = draining.get()
}
