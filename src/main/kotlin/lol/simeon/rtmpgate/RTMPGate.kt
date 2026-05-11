package lol.simeon.rtmpgate

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
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
    val appScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
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

    Runtime.getRuntime().addShutdownHook(
        Thread {
            logger.info("Shutting down RTMPGate")
            appState.beginShutdown()
            runCatching { routeStore.close() }
        },
    )

    appScope.launch {
        httpServer.start(wait = true)
    }

    rtmpRelayServer.startBlocking()
}
