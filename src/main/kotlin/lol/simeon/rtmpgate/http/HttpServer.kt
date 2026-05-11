package lol.simeon.rtmpgate.http

import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import io.ktor.server.plugins.calllogging.CallLogging
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.plugins.swagger.swaggerUI
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Routing
import io.ktor.server.routing.RoutingContext
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import lol.simeon.rtmpgate.config.AppConfig
import lol.simeon.rtmpgate.metrics.RtmpGateMetrics
import lol.simeon.rtmpgate.routes.RouteRecord
import lol.simeon.rtmpgate.routes.RouteStore
import lol.simeon.rtmpgate.rtmp.ActiveRtmpSessionResponse
import lol.simeon.rtmpgate.rtmp.RtmpSessionRegistry
import lol.simeon.rtmpgate.runtime.AppState
import org.slf4j.LoggerFactory

class HttpServer(
    private val config: AppConfig,
    private val routeStore: RouteStore,
    private val sessionRegistry: RtmpSessionRegistry,
    private val appState: AppState,
) {
    private val logger = LoggerFactory.getLogger(HttpServer::class.java)

    fun start(wait: Boolean = true): EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration> {
        return embeddedServer(
            factory = Netty,
            host = config.httpHost,
            port = config.httpPort,
            module = { module() },
        ).start(wait = wait)
    }

    private fun Application.module() {
        installCorePlugins()
        routing {
            swaggerRoutes()
            serviceRoutes()
            routeRoutes()
            sessionRoutes()
        }
    }

    private fun Application.installCorePlugins() {
        install(CallLogging)

        install(ContentNegotiation) {
            json(
                Json {
                    prettyPrint = false
                    ignoreUnknownKeys = true
                    encodeDefaults = true
                },
            )
        }

        install(StatusPages) {
            exception<IllegalArgumentException> { call, cause ->
                call.respond(HttpStatusCode.BadRequest, ErrorResponse(cause.message ?: "Bad request"))
            }
            exception<Throwable> { call, cause ->
                logger.error("Unhandled HTTP error", cause)
                call.respond(HttpStatusCode.InternalServerError, ErrorResponse("Internal server error"))
            }
        }
    }

    private fun Routing.serviceRoutes() {
        get("/") {
            call.respond(ServiceInfoResponse(name = "RTMPGate", status = "ok", version = "1.0.0"))
        }

        get("/health") {
            respondHealth()
        }

        get("/metrics") {
            call.respondText(
                text = RtmpGateMetrics.prometheus(),
                contentType = ContentType.Text.Plain,
            )
        }
    }

    private fun Routing.routeRoutes() {
        route("/v1/routes") {
            get { listRoutes() }
            post { createRoute() }
        }

        route("/v1/routes/{streamKey}") {
            put { upsertRoute() }
            get { getRoute() }
            delete { deleteRoute() }
        }
    }

    private suspend fun RoutingContext.listRoutes() {
        val routes = routeStore.list()

        call.respond(
            RouteListResponse(
                count = routes.size,
                routes = routes,
            ),
        )
    }

    private suspend fun RoutingContext.createRoute() {
        if (!HttpAuth.requireAdmin(call, config)) return

        val request = call.receive<CreateRouteRequest>()
        val streamKey = RouteValidator.requireValidStreamKey(request.streamKey)
        val target = RouteValidator.requireValidTarget(
            request.target,
            config.targetHostAllowList,
        )

        respondUpsertedRoute(streamKey, target)
    }

    private suspend fun RoutingContext.upsertRoute() {
        if (!HttpAuth.requireAdmin(call, config)) return

        val streamKey = call.validStreamKeyParameter()
        val request = call.receive<UpsertRouteRequest>()

        validateBodyStreamKey(request.streamKey, streamKey)

        val target = RouteValidator.requireValidTarget(
            request.target,
            config.targetHostAllowList,
        )

        respondUpsertedRoute(streamKey, target)
    }

    private suspend fun RoutingContext.getRoute() {
        val streamKey = call.validStreamKeyParameter()
        val record = routeStore.get(streamKey)

        if (record == null) {
            call.respond(HttpStatusCode.NotFound, ErrorResponse("Route not found"))
            return
        }

        call.respond(record)
    }

    private suspend fun RoutingContext.deleteRoute() {
        if (!HttpAuth.requireAdmin(call, config)) return

        val streamKey = call.validStreamKeyParameter()
        val deleted = routeStore.delete(streamKey)

        call.respond(
            if (deleted) HttpStatusCode.OK else HttpStatusCode.NotFound,
            DeleteRouteResponse(deleted = deleted),
        )
    }

    private fun Routing.swaggerRoutes() {
        swaggerUI(
            path = "swagger",
            swaggerFile = "openapi/rtmpgate.yaml",
        )
    }

    private fun validateBodyStreamKey(
        bodyStreamKey: String?,
        routeStreamKey: String,
    ) {
        val validatedBodyStreamKey = bodyStreamKey
            ?.let(RouteValidator::requireValidStreamKey)

        require(validatedBodyStreamKey == null || validatedBodyStreamKey == routeStreamKey) {
            "Body streamKey must match the route stream key"
        }
    }

    private fun Routing.sessionRoutes() {
        get("/v1/sessions") {
            val sessions = sessionRegistry.list()
            call.respond(SessionListResponse(count = sessions.size, sessions = sessions))
        }

        delete("/v1/sessions/{sessionId}") {
            if (!HttpAuth.requireAdmin(call, config)) return@delete

            val sessionId = call.parameters["sessionId"].orEmpty()
            val closed = sessionId.isNotBlank() && sessionRegistry.close(sessionId)
            call.respond(if (closed) HttpStatusCode.OK else HttpStatusCode.NotFound, DeleteSessionResponse(closed = closed))
        }
    }

    private suspend fun RoutingContext.respondUpsertedRoute(streamKey: String, target: String) {
        val record = upsertRecord(streamKey, target)
        call.respond(if (record.created) HttpStatusCode.Created else HttpStatusCode.OK, record.route)
    }

    private suspend fun upsertRecord(streamKey: String, target: String): UpsertedRoute {
        val now = System.currentTimeMillis()
        val existing = routeStore.get(streamKey)
        val record = RouteRecord(
            streamKey = streamKey,
            target = target,
            createdAtEpochMillis = existing?.createdAtEpochMillis ?: now,
            updatedAtEpochMillis = now,
        )
        routeStore.put(record)
        return UpsertedRoute(route = record, created = existing == null)
    }

    private suspend fun RoutingContext.respondHealth() {
        val storageReady = routeStore.isReady()
        val acceptingTraffic = storageReady && !appState.isShuttingDown()
        val response = HealthResponse(
            status = if (acceptingTraffic) "ok" else "not_ready",
            storage = if (storageReady) "ok" else "not_ready",
            acceptingTraffic = acceptingTraffic,
            activeSessions = sessionRegistry.count(),
            shuttingDown = appState.isShuttingDown(),
        )

        call.respond(if (acceptingTraffic) HttpStatusCode.OK else HttpStatusCode.ServiceUnavailable, response)
    }

    private fun io.ktor.server.application.ApplicationCall.validStreamKeyParameter(): String {
        return RouteValidator.requireValidStreamKey(parameters["streamKey"].orEmpty())
    }
}

private data class UpsertedRoute(
    val route: RouteRecord,
    val created: Boolean,
)

@Serializable
data class CreateRouteRequest(
    val streamKey: String,
    val target: String,
)

@Serializable
data class UpsertRouteRequest(
    val target: String,
    val streamKey: String? = null,
)

@Serializable
data class RouteListResponse(
    val count: Int,
    val routes: List<RouteRecord>,
)

@Serializable
data class SessionListResponse(
    val count: Int,
    val sessions: List<ActiveRtmpSessionResponse>,
)

@Serializable
data class DeleteRouteResponse(
    val deleted: Boolean,
)

@Serializable
data class DeleteSessionResponse(
    val closed: Boolean,
)

@Serializable
data class ServiceInfoResponse(
    val name: String,
    val status: String,
    val version: String,
)

@Serializable
data class HealthResponse(
    val status: String,
    val storage: String,
    val acceptingTraffic: Boolean,
    val activeSessions: Int,
    val shuttingDown: Boolean,
)

@Serializable
data class ErrorResponse(
    val error: String,
)
