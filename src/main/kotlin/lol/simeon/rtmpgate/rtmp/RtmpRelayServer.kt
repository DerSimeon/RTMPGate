package lol.simeon.rtmpgate.rtmp

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelOption
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.nio.NioServerSocketChannel
import lol.simeon.rtmpgate.config.AppConfig
import lol.simeon.rtmpgate.routes.RouteStore
import lol.simeon.rtmpgate.runtime.AppState
import org.slf4j.LoggerFactory

class RtmpRelayServer(
    private val config: AppConfig,
    private val routeStore: RouteStore,
    private val sessionRegistry: RtmpSessionRegistry,
    private val appState: AppState,
) {
    private val logger = LoggerFactory.getLogger(RtmpRelayServer::class.java)

    fun start(): RtmpRelayHandle {
        val bossGroup = MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())
        val workerGroup = MultiThreadIoEventLoopGroup(NioIoHandler.newFactory())

        return try {
            val bootstrap = ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel::class.java)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.AUTO_READ, true)
                .childHandler(RtmpChannelInitializer(config, routeStore, sessionRegistry, appState))

            val channel = bootstrap.bind(config.rtmpHost, config.rtmpPort).sync().channel()
            val handle = RtmpRelayHandle(channel, bossGroup, workerGroup)
            logger.info("RTMP relay listening on {}:{}", config.rtmpHost, handle.boundPort)
            handle
        } catch (error: Throwable) {
            bossGroup.shutdownGracefully()
            workerGroup.shutdownGracefully()
            throw error
        }
    }

    fun startBlocking() {
        start().use { handle ->
            handle.awaitClose()
        }
    }
}
