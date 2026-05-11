package lol.simeon.rtmpgate.http

import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.respond
import lol.simeon.rtmpgate.config.AppConfig

object HttpAuth {
    suspend fun requireAdmin(call: ApplicationCall, config: AppConfig): Boolean {
        val expected = config.adminToken ?: return true
        val actual = call.request.headers[HttpHeaders.Authorization]
            ?.removePrefix("Bearer ")
            ?.trim()

        if (actual == expected) return true

        call.respond(HttpStatusCode.Unauthorized, ErrorResponse("Missing or invalid bearer token"))
        return false
    }
}
