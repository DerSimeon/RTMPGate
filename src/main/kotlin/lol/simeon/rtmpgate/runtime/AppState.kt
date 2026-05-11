package lol.simeon.rtmpgate.runtime

import java.util.concurrent.atomic.AtomicBoolean

class AppState {
    private val shuttingDown = AtomicBoolean(false)

    fun beginShutdown() {
        shuttingDown.set(true)
    }

    fun isShuttingDown(): Boolean = shuttingDown.get()
}
