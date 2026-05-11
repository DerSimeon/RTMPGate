package lol.simeon.rtmpgate.rtmp

import java.net.URI

data class RtmpTarget(
    val host: String,
    val port: Int,
    val app: String,
    val streamKey: String,
    val tcUrl: String,
) {
    companion object {
        fun parse(url: String, fallbackApp: String, fallbackStreamKey: String): RtmpTarget {
            val uri = URI(url)
            require(uri.scheme == "rtmp") { "Only rtmp:// targets are supported" }
            val host = requireNotNull(uri.host) { "Missing RTMP target host" }
            val port = if (uri.port > 0) uri.port else 1935
            val parts = uri.path.trim('/').split('/').filter { it.isNotBlank() }
            val app = parts.firstOrNull() ?: fallbackApp
            val streamKey = parts.drop(1).joinToString("/").ifBlank { fallbackStreamKey }
            return RtmpTarget(
                host = host,
                port = port,
                app = app,
                streamKey = streamKey,
                tcUrl = "rtmp://$host:$port/$app",
            )
        }
    }
}
