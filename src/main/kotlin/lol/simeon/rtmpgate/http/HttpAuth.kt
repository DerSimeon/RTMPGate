package lol.simeon.rtmpgate.http

import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.respond
import lol.simeon.rtmpgate.config.AppConfig
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

object HttpAuth {
    /**
     * Guards write endpoints. When no admin token is configured, all writes are allowed
     * (single-tenant / private-network default). Otherwise a matching bearer token is required.
     */
    suspend fun requireAdmin(call: ApplicationCall, config: AppConfig): Boolean {
        val expected = config.adminToken ?: return true
        return authorize(call, expected)
    }

    /**
     * Guards read endpoints only when [AppConfig.requireReadAuth] is enabled. This is
     * defense-in-depth for the route/session listings, which expose upstream targets, stream
     * keys, and client IPs.
     */
    suspend fun requireRead(call: ApplicationCall, config: AppConfig): Boolean {
        if (!config.requireReadAuth) return true
        val expected = config.adminToken ?: return true
        return authorize(call, expected)
    }

    private suspend fun authorize(call: ApplicationCall, expected: String): Boolean {
        val actual = call.request.headers[HttpHeaders.Authorization]
            ?.removePrefix("Bearer ")
            ?.trim()

        if (actual != null && constantTimeEquals(actual, expected)) return true

        call.respond(HttpStatusCode.Unauthorized, ErrorResponse("Missing or invalid bearer token"))
        return false
    }

    /**
     * Constant-time comparison to avoid leaking the token via response-time side channels.
     * [MessageDigest.isEqual] on modern JDKs is implemented without early exit.
     */
    private fun constantTimeEquals(a: String, b: String): Boolean {
        return MessageDigest.isEqual(
            a.toByteArray(StandardCharsets.UTF_8),
            b.toByteArray(StandardCharsets.UTF_8),
        )
    }
}
