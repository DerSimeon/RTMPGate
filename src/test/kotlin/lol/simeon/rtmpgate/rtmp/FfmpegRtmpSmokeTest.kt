package lol.simeon.rtmpgate.rtmp

import lol.simeon.rtmpgate.routes.InMemoryRouteStore
import lol.simeon.rtmpgate.testConfig
import org.junit.jupiter.api.Assumptions.assumeTrue
import kotlin.test.Test
import kotlin.test.assertTrue

class FfmpegRtmpSmokeTest {
    @Test
    fun `ffmpeg can reach publish and receives bad stream key for missing route`() {
        assumeTrue(commandExists("ffmpeg"), "ffmpeg is not installed")

        val config = testConfig(rtmpDebug = false)
        RtmpRelayServer(config, InMemoryRouteStore(), RtmpSessionRegistry(), lol.simeon.rtmpgate.runtime.AppState()).start().use {
            val process = ProcessBuilder(
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "error",
                "-re",
                "-f", "lavfi",
                "-i", "testsrc=size=160x90:rate=5",
                "-t", "1",
                "-c:v", "libx264",
                "-preset", "ultrafast",
                "-f", "flv",
                "rtmp://127.0.0.1:${config.rtmpPort}/live/missing-key",
            )
                .redirectErrorStream(true)
                .start()

            val output = process.inputStream.bufferedReader().readText()
            val exitCode = process.waitFor()

            assertTrue(exitCode != 0, "ffmpeg should fail because the route is missing")
            assertTrue(
                output.contains("Unknown stream key", ignoreCase = true) ||
                    output.contains("Operation not permitted", ignoreCase = true),
                "ffmpeg reached the server but did not report the expected publish rejection. Output: $output",
            )
        }
    }

    private fun commandExists(command: String): Boolean {
        return runCatching {
            ProcessBuilder(command, "-version")
                .redirectErrorStream(true)
                .start()
                .waitFor() == 0
        }.getOrDefault(false)
    }
}
