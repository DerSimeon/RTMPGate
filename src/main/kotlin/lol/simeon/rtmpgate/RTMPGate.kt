package lol.simeon.rtmpgate

import lol.simeon.rtmpgate.config.AppConfig
import lol.simeon.rtmpgate.http.HttpServer
import lol.simeon.rtmpgate.routes.CachedRouteStore
import lol.simeon.rtmpgate.routes.InMemoryRouteStore
import lol.simeon.rtmpgate.routes.RedisRouteStore
import lol.simeon.rtmpgate.routes.RouteStore
import lol.simeon.rtmpgate.rtmp.RtmpRelayServer
import lol.simeon.rtmpgate.rtmp.RtmpSessionRegistry
import lol.simeon.rtmpgate.runtime.AppState
import org.slf4j.LoggerFactory

fun main() {
    val logger = LoggerFactory.getLogger("RTMPGate")
    val config = AppConfig.fromEnvironment()
    val appState = AppState()
    val sessionRegistry = RtmpSessionRegistry()

    val backingStore: RouteStore = when (config.storageBackend.lowercase()) {
        "memory", "in-memory", "inmemory" -> InMemoryRouteStore()
        "redis", "valkey" -> RedisRouteStore(config.redisUrl)
        else -> error("Unsupported storage backend: ${config.storageBackend}")
    }

    val routeStore = if (config.routeCacheSeconds > 0) {
        CachedRouteStore(backingStore, config.routeCacheSeconds)
    } else {
        backingStore
    }

    val httpServer = HttpServer(
        config = config,
        routeStore = routeStore,
        sessionRegistry = sessionRegistry,
        appState = appState,
    )

    val rtmpRelayServer = RtmpRelayServer(
        config = config,
        routeStore = routeStore,
        sessionRegistry = sessionRegistry,
        appState = appState,
    )

    // Start HTTP without blocking so we retain the engine handle for an orderly stop.
    val httpEngine = httpServer.start(wait = false)
    val rtmpHandle = rtmpRelayServer.start()

    Runtime.getRuntime().addShutdownHook(
        Thread {
            logger.info("Shutting down RTMPGate (graceful window {}ms)", config.gracefulShutdownMillis)

            // 1. Flip readiness to 503 so load balancers stop routing new traffic to this pod.
            appState.beginShutdown()

            // 2. Stop accepting new RTMP connections; existing sessions keep relaying.
            rtmpHandle.stopAccepting()

            // 3. Drain active sessions, up to the grace window.
            val deadline = System.currentTimeMillis() + config.gracefulShutdownMillis
            while (sessionRegistry.count() > 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(DRAIN_POLL_MILLIS)
            }

            val remaining = sessionRegistry.count()
            if (remaining > 0) {
                logger.warn("Force-closing {} RTMP session(s) after grace window", remaining)
                sessionRegistry.closeAll()
            }

            // 4. Tear down transports and the control plane, then close the route store.
            runCatching { rtmpHandle.close() }
            runCatching { httpEngine.stop(HTTP_STOP_GRACE_MILLIS, config.gracefulShutdownMillis) }
            runCatching { routeStore.close() }

            logger.info("RTMPGate shutdown complete")
        },
    )

    // Block the main thread until the RTMP listener channel closes.
    rtmpHandle.awaitClose()
}

private const val DRAIN_POLL_MILLIS = 100L
private const val HTTP_STOP_GRACE_MILLIS = 1_000L
