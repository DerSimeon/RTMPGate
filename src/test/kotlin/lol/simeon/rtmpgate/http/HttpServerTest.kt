package lol.simeon.rtmpgate.http

import kotlinx.coroutines.runBlocking
import lol.simeon.rtmpgate.routes.InMemoryRouteStore
import lol.simeon.rtmpgate.rtmp.RtmpSessionRegistry
import lol.simeon.rtmpgate.runtime.AppState
import lol.simeon.rtmpgate.testConfig
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals

class HttpServerTest {
    private val client = HttpClient.newHttpClient()

    @Test
    fun `route api supports create get list delete and metrics`() = runBlocking {
        val config = testConfig()
        val store = InMemoryRouteStore()
        val engine = HttpServer(config, store, RtmpSessionRegistry(), AppState()).start(wait = false)

        try {
            val base = "http://127.0.0.1:${config.httpPort}"
            val put = request(
                method = "PUT",
                url = "$base/v1/routes/test-key",
                body = """{"target":"rtmp://127.0.0.1:1936/live/test-target"}""",
            )
            assertEquals(201, put.statusCode())
            assertContains(put.body(), "test-key")

            val get = request("GET", "$base/v1/routes/test-key")
            assertEquals(200, get.statusCode())
            assertContains(get.body(), "test-target")

            val list = request("GET", "$base/v1/routes")
            assertEquals(200, list.statusCode())
            assertContains(list.body(), "\"count\":1")

            val metrics = request("GET", "$base/metrics")
            assertEquals(200, metrics.statusCode())
            assertContains(metrics.body(), "rtmpgate_active_sessions")

            val delete = request("DELETE", "$base/v1/routes/test-key")
            assertEquals(200, delete.statusCode())

            val getMissing = request("GET", "$base/v1/routes/test-key")
            assertEquals(404, getMissing.statusCode())
        } finally {
            engine.stop(1_000, 2_000)
        }
    }

    @Test
    fun `single health endpoint reports readiness`() = runBlocking {
        val config = testConfig()
        val engine = HttpServer(config, InMemoryRouteStore(), RtmpSessionRegistry(), AppState()).start(wait = false)

        try {
            val response = request("GET", "http://127.0.0.1:${config.httpPort}/health")
            assertEquals(200, response.statusCode())
            assertContains(response.body(), "\"acceptingTraffic\":true")
        } finally {
            engine.stop(1_000, 2_000)
        }
    }

    @Test
    fun `write endpoints require bearer token when configured`() = runBlocking {
        val config = testConfig(adminToken = "secret")
        val engine = HttpServer(config, InMemoryRouteStore(), RtmpSessionRegistry(), AppState()).start(wait = false)

        try {
            val base = "http://127.0.0.1:${config.httpPort}"
            val unauthorized = request(
                method = "PUT",
                url = "$base/v1/routes/test-key",
                body = """{"target":"rtmp://127.0.0.1:1936/live/test-target"}""",
            )
            assertEquals(401, unauthorized.statusCode())

            val authorized = request(
                method = "PUT",
                url = "$base/v1/routes/test-key",
                body = """{"target":"rtmp://127.0.0.1:1936/live/test-target"}""",
                token = "secret",
            )
            assertEquals(201, authorized.statusCode())
        } finally {
            engine.stop(1_000, 2_000)
        }
    }

    @Test
    fun `route api rejects invalid targets`() = runBlocking {
        val config = testConfig()
        val engine = HttpServer(config, InMemoryRouteStore(), RtmpSessionRegistry(), AppState()).start(wait = false)

        try {
            val response = request(
                method = "PUT",
                url = "http://127.0.0.1:${config.httpPort}/v1/routes/test-key",
                body = """{"target":"http://127.0.0.1/live/test-target"}""",
            )
            assertEquals(400, response.statusCode())
            assertContains(response.body(), "Only rtmp:// targets are supported")
        } finally {
            engine.stop(1_000, 2_000)
        }
    }

    private fun request(method: String, url: String, body: String? = null, token: String? = null): HttpResponse<String> {
        val builder = HttpRequest.newBuilder(URI.create(url))
        token?.let { builder.header("Authorization", "Bearer $it") }

        if (body == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody())
        } else {
            builder.header("Content-Type", "application/json")
            builder.method(method, HttpRequest.BodyPublishers.ofString(body))
        }

        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    }
}
