package lol.simeon.rtmpgate.rtmp

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import lol.simeon.rtmpgate.config.AppConfig
import lol.simeon.rtmpgate.metrics.RtmpGateMetrics
import lol.simeon.rtmpgate.routes.RouteStore
import lol.simeon.rtmpgate.runtime.AppState
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import kotlin.system.measureNanoTime

class RtmpSession(
    private val config: AppConfig,
    private val routeStore: RouteStore,
    private val sessionRegistry: RtmpSessionRegistry,
    private val appState: AppState,
) : SimpleChannelInboundHandler<ByteBuf>() {
    private val logger = LoggerFactory.getLogger(RtmpSession::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val codec = RtmpChunkCodec()
    private val inputBuffer = Unpooled.buffer()
    private val bufferedRelayMessages = mutableListOf<RtmpMessage>()
    private val relayLock = Any()

    private var state = RtmpSessionState.WAIT_C0_C1
    private var appName = "live"
    private var clientStreamId = 1
    private var upstream: RtmpUpstreamClient? = null
    private var relayMetricOpen = false
    private var sessionId: String? = null
    private var closeReason = "client_disconnected"

    override fun channelActive(ctx: ChannelHandlerContext) {
        if (appState.isShuttingDown()) {
            reject(ctx, "shutting_down")
            return
        }

        val remoteIp = ((ctx.channel().remoteAddress() as? InetSocketAddress)?.address?.hostAddress) ?: "unknown"

        if (config.maxActiveSessions > 0 && sessionRegistry.count() >= config.maxActiveSessions) {
            reject(ctx, "max_sessions")
            return
        }

        if (config.maxSessionsPerIp > 0 && sessionRegistry.countByIp(remoteIp) >= config.maxSessionsPerIp) {
            reject(ctx, "max_sessions_per_ip")
            return
        }

        val session = sessionRegistry.register(ctx.channel())
        sessionId = session.id
        RtmpGateMetrics.sessionAccepted()
    }

    override fun channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf) {
        inputBuffer.writeBytes(msg)
        runCatching { drain(ctx) }
            .onFailure { error ->
                closeReason = "protocol_error"
                logger.warn("Closing RTMP session: {}", error.message)
                close(ctx)
            }

        if (inputBuffer.refCnt() > 0 && inputBuffer.readerIndex() > 0) {
            inputBuffer.discardReadBytes()
        }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        closeReason = "connection_error"
        if (config.rtmpDebug) {
            logger.warn("RTMP session error", cause)
        } else {
            logger.info("RTMP session closed after connection error: {}", cause.message)
        }
        close(ctx)
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        upstream?.close()
        if (relayMetricOpen) {
            relayMetricOpen = false
            RtmpGateMetrics.relayClosed()
        }

        sessionId?.let(sessionRegistry::unregister)
        RtmpGateMetrics.sessionClosed(closeReason)

        if (inputBuffer.refCnt() > 0) {
            inputBuffer.release()
        }
    }

    private fun drain(ctx: ChannelHandlerContext) {
        while (inputBuffer.isReadable && state != RtmpSessionState.CLOSED) {
            debug("RTMP state={} readable={}", state, inputBuffer.readableBytes())

            val shouldContinue = when (state) {
                RtmpSessionState.WAIT_C0_C1 -> drainC0C1(ctx)
                RtmpSessionState.WAIT_C2 -> drainC2()
                RtmpSessionState.WAIT_PUBLISH -> drainPublish(ctx)
                RtmpSessionState.STARTING_UPSTREAM -> drainStartingUpstream()
                RtmpSessionState.RELAYING -> drainRelaying()
                RtmpSessionState.CLOSED -> false
            }

            if (!shouldContinue) return
        }
    }

    private fun drainC0C1(ctx: ChannelHandlerContext): Boolean {
        if (!RtmpHandshake.canReadC0C1(inputBuffer)) return false

        RtmpHandshake.handleServerHandshake(ctx, inputBuffer)
        state = RtmpSessionState.WAIT_C2
        return true
    }

    private fun drainC2(): Boolean {
        if (!RtmpHandshake.canReadC2(inputBuffer)) return false

        RtmpHandshake.discardC2(inputBuffer)
        state = RtmpSessionState.WAIT_PUBLISH
        return true
    }

    private fun drainPublish(ctx: ChannelHandlerContext): Boolean {
        val messages = readMessagesOrNull() ?: return false

        handleHandshakeMessages(ctx, messages)
        return true
    }

    private fun drainStartingUpstream(): Boolean {
        val messages = readMessagesOrNull() ?: return false
        val relayableMessages = messages.filter { it.isRelayableMedia() }

        synchronized(relayLock) {
            bufferedRelayMessages += relayableMessages
        }

        return true
    }

    private fun drainRelaying(): Boolean {
        val messages = readMessagesOrNull() ?: return false
        val client = upstream ?: return false

        messages
            .filter { it.isRelayableMedia() }
            .forEach { message -> relayMessage(client, message) }

        return true
    }

    private fun readMessagesOrNull(): List<RtmpMessage>? {
        val messages = codec.readMessages(inputBuffer)
        return messages.ifEmpty { null }
    }

    private fun relayMessage(client: RtmpUpstreamClient, message: RtmpMessage) {
        val payloadSize = message.payload.size.toLong()

        RtmpGateMetrics.bytesIn(payloadSize)
        client.writeMedia(message)
        RtmpGateMetrics.bytesOut(payloadSize)
    }

    private fun handleHandshakeMessages(ctx: ChannelHandlerContext, messages: List<RtmpMessage>) {
        for (message in messages) {
            debug("RTMP message type={} streamId={} payloadSize={}", message.typeId, message.streamId, message.payload.size)

            when (message.typeId) {
                RtmpConstants.MSG_SET_CHUNK_SIZE -> debug("Client changed inbound chunk size")

                RtmpConstants.MSG_COMMAND_AMF0, RtmpConstants.MSG_COMMAND_AMF3 -> {
                    codec.command(message)?.let { command -> handleCommand(ctx, message, command) }
                }
            }
        }
    }

    private fun handleCommand(ctx: ChannelHandlerContext, message: RtmpMessage, command: RtmpCommand) {
        debug(
            "RTMP command name={} transactionId={} app={} streamKey={}",
            command.name,
            command.transactionId,
            command.app,
            command.streamKey,
        )

        when (command.name) {
            "connect" -> {
                appName = command.app?.takeIf { it.isNotBlank() } ?: appName
                ctx.write(RtmpServerResponses.windowAcknowledgementSize())
                ctx.write(RtmpServerResponses.peerBandwidth())
                ctx.write(RtmpServerResponses.connectResult(command.transactionId))
                ctx.flush()
            }

            "releaseStream" -> Unit

            "FCPublish" -> {
                ctx.writeAndFlush(RtmpServerResponses.onFCPublish(command.transactionId, command.streamKey))
            }

            "createStream" -> {
                clientStreamId = 1
                ctx.write(RtmpServerResponses.streamBegin(clientStreamId))
                ctx.write(RtmpServerResponses.createStreamResult(command.transactionId, clientStreamId))
                ctx.flush()
            }

            "publish" -> {
                RtmpGateMetrics.publishAttempt()
                clientStreamId = message.streamId.takeIf { it > 0 } ?: clientStreamId
                val streamKey = command.streamKey?.takeIf { it.isNotBlank() }

                if (streamKey == null) {
                    logger.info("Rejecting publish without stream key")
                    RtmpGateMetrics.publishRejected("missing_stream_key")
                    closeReason = "missing_stream_key"
                    close(ctx)
                    return
                }

                startRelay(ctx, RtmpPublishInfo(app = appName, streamKey = streamKey, clientStreamId = clientStreamId))
            }
        }
    }

    private fun startRelay(ctx: ChannelHandlerContext, publishInfo: RtmpPublishInfo) {
        if (state == RtmpSessionState.STARTING_UPSTREAM || state == RtmpSessionState.RELAYING) return

        state = RtmpSessionState.STARTING_UPSTREAM
        sessionId?.let { sessionRegistry.updatePublish(it, publishInfo.streamKey, target = null, state = "starting_upstream") }

        scope.launch {
            var route: lol.simeon.rtmpgate.routes.RouteRecord? = null
            val lookupNanos = measureNanoTime {
                route = routeStore.get(publishInfo.streamKey)
            }
            RtmpGateMetrics.routeLookup(lookupNanos)

            if (route == null) {
                logger.info("Rejecting stream key {} because no route exists", publishInfo.streamKey)
                RtmpGateMetrics.publishRejected("unknown_stream_key")
                closeReason = "unknown_stream_key"
                ctx.writeAndFlush(RtmpServerResponses.publishRejected(publishInfo.clientStreamId, publishInfo.streamKey))
                    .addListener { close(ctx) }
                return@launch
            }

            val target = route.target
            sessionId?.let { sessionRegistry.updatePublish(it, publishInfo.streamKey, target, "connecting_upstream") }

            runCatching {
                val client = RtmpUpstreamClient(
                    targetUrl = target,
                    sourceApp = publishInfo.app,
                    sourceStreamKey = publishInfo.streamKey,
                    config = config,
                )
                client.connectAndPublish()
                upstream = client

                ctx.writeAndFlush(RtmpServerResponses.publishStart(publishInfo.clientStreamId, publishInfo.streamKey))

                val toFlush = synchronized(relayLock) {
                    val copy = bufferedRelayMessages.toList()
                    bufferedRelayMessages.clear()
                    copy
                }
                toFlush.forEach {
                    RtmpGateMetrics.bytesIn(it.payload.size.toLong())
                    client.writeMedia(it)
                    RtmpGateMetrics.bytesOut(it.payload.size.toLong())
                }

                state = RtmpSessionState.RELAYING
                sessionId?.let { sessionRegistry.updatePublish(it, publishInfo.streamKey, target, "relaying") }
                relayMetricOpen = true
                RtmpGateMetrics.publishAccepted()
                logger.info("Relaying streamKey={} target={}", publishInfo.streamKey, target)
            }.onFailure { error ->
                logger.warn("Failed to start upstream relay streamKey={} target={}: {}", publishInfo.streamKey, target, error.message)
                RtmpGateMetrics.upstreamFailure()
                RtmpGateMetrics.publishRejected("upstream_unavailable")
                closeReason = "upstream_unavailable"
                ctx.writeAndFlush(RtmpServerResponses.publishRejected(publishInfo.clientStreamId, publishInfo.streamKey))
                    .addListener { close(ctx) }
            }
        }
    }

    private fun RtmpMessage.isRelayableMedia(): Boolean {
        return typeId == RtmpConstants.MSG_AUDIO ||
            typeId == RtmpConstants.MSG_VIDEO ||
            typeId == RtmpConstants.MSG_DATA_AMF0 ||
            typeId == RtmpConstants.MSG_DATA_AMF3
    }

    private fun reject(ctx: ChannelHandlerContext, reason: String) {
        closeReason = reason
        RtmpGateMetrics.sessionRejected(reason)
        ctx.close()
    }

    private fun close(ctx: ChannelHandlerContext) {
        if (state == RtmpSessionState.CLOSED) return
        state = RtmpSessionState.CLOSED
        sessionId?.let { sessionRegistry.updateState(it, "closing") }
        upstream?.close()
        if (relayMetricOpen) {
            relayMetricOpen = false
            RtmpGateMetrics.relayClosed()
        }
        ctx.close()
    }

    private fun debug(message: String, vararg args: Any?) {
        if (config.rtmpDebug) {
            logger.info(message, *args)
        }
    }
}
