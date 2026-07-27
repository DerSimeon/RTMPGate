package lol.simeon.rtmpgate.rtmp

import io.netty.channel.Channel
import io.netty.channel.EventLoopGroup

class RtmpRelayHandle(
    private val channel: Channel,
    private val bossGroup: EventLoopGroup,
    private val workerGroup: EventLoopGroup,
) : AutoCloseable {
    val boundPort: Int
        get() = (channel.localAddress() as java.net.InetSocketAddress).port

    fun awaitClose() {
        channel.closeFuture().sync()
    }

    /**
     * Closes the listening socket so no new RTMP connections are accepted, while leaving the
     * event-loop groups (and therefore existing sessions) running for graceful draining.
     */
    fun stopAccepting() {
        runCatching { channel.close().syncUninterruptibly() }
    }

    override fun close() {
        runCatching { channel.close().syncUninterruptibly() }
        runCatching { bossGroup.shutdownGracefully().syncUninterruptibly() }
        runCatching { workerGroup.shutdownGracefully().syncUninterruptibly() }
    }
}
