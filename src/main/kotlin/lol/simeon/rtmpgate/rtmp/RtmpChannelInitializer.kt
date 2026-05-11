package lol.simeon.rtmpgate.rtmp

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.timeout.ReadTimeoutHandler
import lol.simeon.rtmpgate.config.AppConfig
import lol.simeon.rtmpgate.routes.RouteStore
import lol.simeon.rtmpgate.runtime.AppState
import java.util.concurrent.TimeUnit

class RtmpChannelInitializer(
    private val config: AppConfig,
    private val routeStore: RouteStore,
    private val sessionRegistry: RtmpSessionRegistry,
    private val appState: AppState,
) : ChannelInitializer<SocketChannel>() {
    override fun initChannel(channel: SocketChannel) {
        channel.pipeline()
            .addLast("readTimeout", ReadTimeoutHandler(config.readTimeoutMillis.toLong(), TimeUnit.MILLISECONDS))
            .addLast("session", RtmpSession(config, routeStore, sessionRegistry, appState))
    }
}
