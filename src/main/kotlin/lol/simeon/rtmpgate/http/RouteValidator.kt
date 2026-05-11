package lol.simeon.rtmpgate.http

import java.net.URI

object RouteValidator {
    private val streamKeyPattern = Regex("^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$")

    fun requireValidStreamKey(streamKey: String): String {
        val normalized = streamKey.trim()

        require(normalized.isNotEmpty()) { "Missing stream key" }
        require(streamKeyPattern.matches(normalized)) {
            "Stream key must start with a letter or digit and may only contain letters, digits, dots, underscores, " +
                "dashes, colons and at signs. Max length: 128."
        }
        require(!normalized.contains("..")) { "Stream key must not contain '..'" }
        require(normalized != "." && normalized != "-") { "Stream key is too ambiguous" }

        return normalized
    }

    fun requireValidTarget(target: String, hostAllowList: Set<String> = emptySet()): String {
        val normalized = target.trim()
        val uri = runCatching { URI(normalized) }.getOrNull()
            ?: throw IllegalArgumentException("Invalid RTMP target URL")

        require(uri.scheme == "rtmp") { "Only rtmp:// targets are supported" }
        require(!uri.host.isNullOrBlank()) { "RTMP target must include a host" }
        require(uri.rawQuery == null && uri.rawFragment == null) { "RTMP target must not include query or fragment" }
        require(uri.path.trim('/').isNotEmpty()) { "RTMP target must include an app and stream path" }
        require(uri.port == -1 || uri.port in 1..65535) { "RTMP target port is out of range" }

        if (hostAllowList.isNotEmpty()) {
            val host = uri.host.lowercase()
            require(host in hostAllowList) {
                "RTMP target host is not allowed"
            }
        }

        return normalized
    }
}
