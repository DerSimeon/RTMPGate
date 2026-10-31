package lol.simeon.rtmpgate.rtmp

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import lol.simeon.rtmpgate.config.AppConfig
import lol.simeon.rtmpgate.metrics.RtmpGateMetrics
import lol.simeon.rtmpgate.routes.RouteStore
import lol.simeon.rtmpgate.runtime.AppState
import org.slf4j.LoggerFactory
import java.io.IOException
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
    private val codec = RtmpChunkCodec(maxMessageBytes = config.maxRtmpMessageBytes)
    private val inputBuffer = Unpooled.buffer()
    private val bufferedRelayMessages = mutableListOf<RtmpPacket>()
    private val relayLock = Any()
    private var bufferedRelayBytes = 0L
    private val failover = UpstreamFailover(config)

    // All mutable session state below is confined to the Netty channel event loop. The
    // background coroutines (upstream connect in startRelay, upstream reconnect started by
    // onUpstreamFailed) hop back via runOnEventLoop before touching any of it.
    // sessionId/remoteAddress are set once in channelActive (event loop) and read from those
    // coroutines for logging, so they are marked @Volatile for visibility.
    private var state = RtmpSessionState.WAIT_C0_C1
    private var appName = "live"
    private var clientStreamId = 1
    private var upstream: RtmpUpstreamClient? = null
    private var relayMetricOpen = false
    @Volatile
    private var sessionId: String? = null
    private var closeReason = "client_disconnected"
    private var currentStreamKey: String? = null
    private var currentTarget: String? = null
    @Volatile
    private var remoteAddress: String = "unknown"
    private var bytesInTotal = 0L
    private var bytesOutTotal = 0L
    private var lastMessageType: String? = null
    private var backpressureSince: Long? = null

    // Set once the first upstream publish succeeds; needed to reconnect (see onUpstreamFailed).
    private var publishInfo: RtmpPublishInfo? = null

    // The upstream client whose blocking connect is in flight on a background coroutine. The
    // channel closes it on teardown so a cancelled connect does not linger on its socket timeouts.
    @Volatile
    private var connectingClient: RtmpUpstreamClient? = null

    override fun channelActive(ctx: ChannelHandlerContext) {
        if (appState.isDraining()) {
            reject(ctx, "draining")
            return
        }

        val remoteIp = ((ctx.channel().remoteAddress() as? InetSocketAddress)?.address?.hostAddress) ?: "unknown"
        remoteAddress = remoteIp

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

        // Guard against a client that dribbles an incomplete oversized message to exhaust heap.
        if (inputBuffer.readableBytes() > config.maxInputBufferBytes) {
            closeReason = "input_buffer_exceeded"
            logger.warn(
                "Closing RTMP session: unparsed input exceeded limit sessionId={} remote={} bytes={} limit={}",
                sessionId,
                remoteAddress,
                inputBuffer.readableBytes(),
                config.maxInputBufferBytes,
            )
            close(ctx)
            return
        }

        runCatching { drain(ctx) }
            .onFailure { error ->
                val message = error.message.orEmpty()
                val isDisconnect =
                    error is IOException &&
                            (
                                    message.contains("Broken pipe", ignoreCase = true) ||
                                            message.contains("Connection reset", ignoreCase = true) ||
                                            message.contains("Connection reset by peer", ignoreCase = true)
                                    )

                closeReason = if (isDisconnect) {
                    "client_disconnected"
                } else {
                    "protocol_error"
                }

                logger.warn(
                    "Closing RTMP session sessionId={} streamKey={} target={} remote={} reason={} errorType={} message={}",
                    sessionId,
                    currentStreamKey,
                    currentTarget,
                    remoteAddress,
                    closeReason,
                    error::class.simpleName,
                    error.message,
                )

                close(ctx)
            }

        if (inputBuffer.refCnt() > 0 && inputBuffer.readerIndex() > 0) {
            inputBuffer.discardReadBytes()
        }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        val message = cause.message.orEmpty()

        if (
            cause is IOException &&
            (message.contains("Broken pipe", ignoreCase = true) ||
                    message.contains("Connection reset", ignoreCase = true))
        ) {
            closeReason = "client_disconnected"
            logger.info(
                "RTMP client disconnected sessionId={} streamKey={} target={} remote={} reason={} message={}",
                sessionId,
                currentStreamKey,
                currentTarget,
                remoteAddress,
                closeReason,
                message,
            )
            close(ctx)
            return
        }

        closeReason = "connection_error"
        if (config.rtmpDebug) {
            logger.warn("RTMP session error", cause)
        } else {
            logger.info(
                "RTMP session closed after connection error sessionId={} streamKey={} target={} remote={} message={}",
                sessionId,
                currentStreamKey,
                currentTarget,
                remoteAddress,
                message,
            )
        }
        close(ctx)
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        upstream?.close()
        connectingClient?.close()
        if (relayMetricOpen) {
            relayMetricOpen = false
            RtmpGateMetrics.relayClosed()
        }
        // Ends a reconnect gap still open, aborts its in-flight connect and frees cached headers.
        failover.sessionClosed()

        sessionId?.let { sessionRegistry.takeRequestedCloseReason(it) }?.let { closeReason = it }

        logger.info(
            "RTMP session inactive sessionId={} streamKey={} target={} remote={} reason={}",
            sessionId,
            currentStreamKey,
            currentTarget,
            remoteAddress,
            closeReason,
        )

        sessionId?.let(sessionRegistry::unregister)
        RtmpGateMetrics.sessionClosed(closeReason)

        synchronized(relayLock) {
            bufferedRelayMessages.forEach(RtmpPacket::release)
            bufferedRelayMessages.clear()
            bufferedRelayBytes = 0
        }

        if (inputBuffer.refCnt() > 0) {
            inputBuffer.release()
        }

        // Cancel the per-session coroutines (upstream connect and reconnect) so they cannot
        // outlive the channel.
        scope.cancel()
    }

    private fun drain(ctx: ChannelHandlerContext) {
        while (inputBuffer.isReadable && state != RtmpSessionState.CLOSED) {
            debug("RTMP state={} readable={}", state, inputBuffer.readableBytes())

            val shouldContinue = when (state) {
                RtmpSessionState.WAIT_C0_C1 -> drainC0C1(ctx)
                RtmpSessionState.WAIT_C2 -> drainC2()
                RtmpSessionState.WAIT_PUBLISH -> drainPublish(ctx)
                RtmpSessionState.STARTING_UPSTREAM -> drainStartingUpstream(ctx)
                RtmpSessionState.RELAYING, RtmpSessionState.UPSTREAM_RETRYING -> drainMedia(ctx)
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
        val messages = readPacketsOrNull() ?: return false

        handleHandshakeMessages(ctx, messages)
        return true
    }

    private fun drainStartingUpstream(ctx: ChannelHandlerContext): Boolean {
        val messages = readPacketsOrNull() ?: return false
        val relayableMessages = messages.filter { it.isRelayableMedia() }
        relayableMessages.forEach(failover.relayResume::observe)

        synchronized(relayLock) {
            relayableMessages.forEach { message ->
                bufferedRelayMessages += message
                bufferedRelayBytes += message.payloadSize.toLong()
            }

            RtmpGateMetrics.startupBuffer(
                bufferedRelayMessages.size.toLong(),
                bufferedRelayBytes,
            )

            val exceededMessages = bufferedRelayMessages.size > config.startupBufferMessages
            val exceededBytes = bufferedRelayBytes > config.startupBufferBytes

            if (exceededMessages || exceededBytes) {
                logger.warn(
                    "Startup buffer exceeded sessionId={} streamKey={} messages={} bytes={}",
                    sessionId,
                    currentStreamKey,
                    bufferedRelayMessages.size,
                    bufferedRelayBytes,
                )

                RtmpGateMetrics.startupBufferExceeded()
                closeReason = "startup_buffer_exceeded"
                close(ctx)
                return false
            }
        }

        return true
    }

    /**
     * RELAYING and UPSTREAM_RETRYING. The state is re-checked for every packet: once a packet
     * closes the session or moves it into the retry state, the rest of the batch is no longer
     * relayed (so an upstream failure is handled, and logged, once). Every packet is released.
     */
    private fun drainMedia(ctx: ChannelHandlerContext): Boolean {
        val messages = readPacketsOrNull() ?: return false

        messages.forEach { message ->
            try {
                if (message.isRelayableMedia()) handleMediaMessage(ctx, message)
            } finally {
                message.release()
            }
        }

        return state != RtmpSessionState.CLOSED
    }

    private fun handleMediaMessage(ctx: ChannelHandlerContext, message: RtmpPacket) {
        when (state) {
            RtmpSessionState.RELAYING -> {
                failover.relayResume.observe(message)
                relayMessage(ctx, message)
            }

            RtmpSessionState.UPSTREAM_RETRYING -> {
                // Keep the encoder flowing but drop its media; nothing is buffered across the gap.
                failover.relayResume.observe(message)
                RtmpGateMetrics.upstreamRetryDroppedMessage()
            }

            else -> Unit
        }
    }

    private fun readPacketsOrNull(): List<RtmpPacket>? {
        val packets = codec.readPackets(inputBuffer)
        return packets.ifEmpty { null }
    }

    private fun relayMessage(ctx: ChannelHandlerContext, message: RtmpPacket) {
        val client = upstream ?: return
        client.failure()?.let { error ->
            onUpstreamFailed(ctx, error)
            return
        }

        if (!applyBackpressure(ctx, client)) return
        if (!failover.relayResume.admit(message)) return

        val payloadSize = message.payloadSize.toLong()
        bytesInTotal += payloadSize
        RtmpGateMetrics.bytesIn(payloadSize)
        if (!client.writeMedia(message)) {
            onUpstreamFailed(ctx, client.failure())
            return
        }
        RtmpGateMetrics.mediaMessageRelayed(message.typeId)
        bytesOutTotal += payloadSize
        RtmpGateMetrics.bytesOut(payloadSize)
        sessionId?.let {
            sessionRegistry.updateRelayStats(
                id = it,
                bytesIn = bytesInTotal,
                bytesOut = bytesOutTotal,
                lastMessageType = message.typeName(),
                upstreamWritable = !client.isBackpressured(),
                backpressureSinceEpochMillis = backpressureSince,
            )
        }
    }

    /** Pauses/resumes encoder reads on upstream backpressure. Returns false if the session was closed. */
    private fun applyBackpressure(ctx: ChannelHandlerContext, client: RtmpUpstreamClient): Boolean {
        if (client.isBackpressured()) {
            if (ctx.channel().config().isAutoRead) {
                ctx.channel().config().isAutoRead = false
                backpressureSince = System.currentTimeMillis()
                RtmpGateMetrics.backpressureEvent()
                logger.warn(
                    "Pausing RTMP inbound reads sessionId={} streamKey={} queueBytes={}",
                    sessionId,
                    currentStreamKey,
                    client.queueBytes(),
                )
            }

            val blockedFor = backpressureSince?.let { System.currentTimeMillis() - it } ?: 0
            if (blockedFor > config.backpressureTimeoutMillis) {
                RtmpGateMetrics.backpressureDisconnect()
                closeReason = "upstream_backpressure_timeout"
                close(ctx)
                return false
            }
        } else if (!ctx.channel().config().isAutoRead) {
            resumeAutoRead(ctx)
        }
        return true
    }

    private fun resumeAutoRead(ctx: ChannelHandlerContext) {
        if (ctx.channel().config().isAutoRead) return
        ctx.channel().config().isAutoRead = true
        backpressureSince = null
        logger.info(
            "Resuming RTMP inbound reads sessionId={} streamKey={}",
            sessionId,
            currentStreamKey,
        )
    }

    private fun handleHandshakeMessages(ctx: ChannelHandlerContext, messages: List<RtmpPacket>) {
        for (message in messages) {
            debug("RTMP message type={} streamId={} payloadSize={}", message.typeId, message.streamId, message.payloadSize)

            when (message.typeId) {
                RtmpConstants.MSG_SET_CHUNK_SIZE -> debug("Client changed inbound chunk size")

                RtmpConstants.MSG_COMMAND_AMF0, RtmpConstants.MSG_COMMAND_AMF3 -> {
                    codec.command(message.toMessage())?.let { command -> handleCommand(ctx, message, command) }
                }
            }
        }
    }

    private fun handleCommand(ctx: ChannelHandlerContext, message: RtmpPacket, command: RtmpCommand) {
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

                currentStreamKey = streamKey
                startRelay(ctx, RtmpPublishInfo(app = appName, streamKey = streamKey, clientStreamId = clientStreamId))
            }
        }
    }

    private fun startRelay(ctx: ChannelHandlerContext, publishInfo: RtmpPublishInfo) {
        if (state != RtmpSessionState.WAIT_PUBLISH) return

        state = RtmpSessionState.STARTING_UPSTREAM
        sessionId?.let { sessionRegistry.updatePublish(it, publishInfo.streamKey, target = null, state = "starting_upstream") }

        // The route lookup (suspend) and the upstream RTMP connect (blocking socket I/O) run off
        // the event loop. Every subsequent mutation of session state and every ctx write is
        // hopped back onto the event loop via runOnEventLoop so the handler stays single-threaded.
        scope.launch {
            var route: lol.simeon.rtmpgate.routes.RouteRecord? = null
            val lookupNanos = measureNanoTime {
                route = routeStore.get(publishInfo.streamKey)
            }
            RtmpGateMetrics.routeLookup(lookupNanos)

            val resolved = route
            if (resolved == null) {
                RtmpGateMetrics.publishRejected("unknown_stream_key")
                runOnEventLoop(ctx) {
                    logger.info(
                        "Rejecting stream key because no route exists sessionId={} streamKey={} remote={}",
                        sessionId,
                        publishInfo.streamKey,
                        remoteAddress,
                    )
                    closeReason = "unknown_stream_key"
                    ctx.writeAndFlush(RtmpServerResponses.publishRejected(publishInfo.clientStreamId, publishInfo.streamKey))
                        .addListener { close(ctx) }
                }
                return@launch
            }

            val target = resolved.target
            runOnEventLoop(ctx) {
                currentTarget = target
                sessionId?.let { sessionRegistry.updatePublish(it, publishInfo.streamKey, target, "connecting_upstream") }
            }

            var client: RtmpUpstreamClient? = null
            runCatching {
                RtmpUpstreamClient(
                    targetUrl = target,
                    sourceApp = publishInfo.app,
                    sourceStreamKey = publishInfo.streamKey,
                    config = config,
                ).also {
                    client = it
                    connectingClient = it
                    it.connectAndPublish()
                }
            }.also {
                connectingClient = null
            }.onSuccess { connected ->
                runOnEventLoop(ctx) { onUpstreamReady(ctx, publishInfo, target, connected) }
            }.onFailure { error ->
                client?.close()
                // The channel closed (and aborted the connect) while we were connecting.
                if (!isActive) return@launch
                RtmpGateMetrics.upstreamFailure()
                RtmpGateMetrics.publishRejected("upstream_unavailable")
                runOnEventLoop(ctx) {
                    logger.error(
                        "Failed to start upstream relay sessionId={} streamKey={} target={} remote={} errorType={} message={}",
                        sessionId,
                        publishInfo.streamKey,
                        target,
                        remoteAddress,
                        error::class.simpleName,
                        error.message,
                        error,
                    )
                    closeReason = "upstream_unavailable"
                    ctx.writeAndFlush(RtmpServerResponses.publishRejected(publishInfo.clientStreamId, publishInfo.streamKey))
                        .addListener { close(ctx) }
                }
            }
        }
    }

    /** Runs on the event loop after the upstream connection is established. */
    private fun onUpstreamReady(
        ctx: ChannelHandlerContext,
        publishInfo: RtmpPublishInfo,
        target: String,
        client: RtmpUpstreamClient,
    ) {
        // The client channel may have closed while we were connecting upstream.
        if (state == RtmpSessionState.CLOSED) {
            client.close()
            return
        }

        upstream = client
        this.publishInfo = publishInfo
        ctx.writeAndFlush(RtmpServerResponses.publishStart(publishInfo.clientStreamId, publishInfo.streamKey))

        val toFlush = synchronized(relayLock) {
            val copy = bufferedRelayMessages.toList()
            bufferedRelayMessages.clear()
            bufferedRelayBytes = 0
            RtmpGateMetrics.startupBuffer(0, 0)
            copy
        }
        toFlush.forEach {
            try {
                RtmpGateMetrics.bytesIn(it.payloadSize.toLong())
                // A false return means the upstream already failed; the next relayed packet
                // sees client.failure() and starts the reconnect.
                if (client.writeMedia(it)) {
                    RtmpGateMetrics.mediaMessageRelayed(it.typeId)
                    RtmpGateMetrics.bytesOut(it.payloadSize.toLong())
                }
            } finally {
                it.release()
            }
        }

        state = RtmpSessionState.RELAYING
        sessionId?.let { sessionRegistry.updatePublish(it, publishInfo.streamKey, target, "relaying") }
        relayMetricOpen = true
        RtmpGateMetrics.publishAccepted()
        logger.info(
            "Relaying sessionId={} streamKey={} target={} remote={}",
            sessionId,
            publishInfo.streamKey,
            target,
            remoteAddress,
        )
    }

    /**
     * Event loop. The upstream failed while relaying. Instead of dropping the encoder, keep it
     * connected and reconnect the upstream in the background for up to
     * RTMPGATE_UPSTREAM_RETRY_WINDOW_MS. With the window set to 0 the session closes as before.
     */
    private fun onUpstreamFailed(ctx: ChannelHandlerContext, error: Throwable?) {
        if (state != RtmpSessionState.RELAYING) return
        val reason = error?.message ?: error?.let { it::class.simpleName } ?: "unknown upstream failure"
        upstream?.close()
        upstream = null

        val info = publishInfo
        val target = currentTarget
        if (config.upstreamRetryWindowMillis <= 0 || info == null || target == null) {
            logger.warn(
                "Closing RTMP session: upstream writer failed sessionId={} streamKey={} target={} message={}",
                sessionId,
                currentStreamKey,
                currentTarget,
                reason,
            )
            closeReason = "upstream_unavailable"
            close(ctx)
            return
        }

        state = RtmpSessionState.UPSTREAM_RETRYING
        // Keep reading the encoder (and dropping its media) even if backpressure had paused it.
        resumeAutoRead(ctx)
        sessionId?.let { sessionRegistry.updateState(it, "upstream_retrying") }

        val request = failover.begin(sessionId, info, target, remoteAddress, reason)
        scope.launch {
            val result = failover.reconnect(request) ?: return@launch
            runOnEventLoop(ctx) { onReconnectResult(ctx, result) }
        }
    }

    /** Event loop. Resumes relaying on the new upstream, or closes the encoder once the window ran out. */
    private fun onReconnectResult(ctx: ChannelHandlerContext, result: UpstreamReconnectResult) {
        if (state != RtmpSessionState.UPSTREAM_RETRYING) {
            (result as? UpstreamReconnectResult.Reconnected)?.client?.close()
            return
        }

        when (result) {
            is UpstreamReconnectResult.Reconnected -> {
                upstream = result.client
                failover.recovered(result.client, result.attempts)
                state = RtmpSessionState.RELAYING
                sessionId?.let { sessionRegistry.updateState(it, "relaying") }
            }

            is UpstreamReconnectResult.Exhausted -> {
                failover.exhausted(result.attempts, result.lastReason)
                closeReason = "upstream_unavailable"
                close(ctx)
            }
        }
    }

    private fun runOnEventLoop(ctx: ChannelHandlerContext, block: () -> Unit) {
        val loop = ctx.channel().eventLoop()
        if (loop.inEventLoop()) block() else loop.execute { block() }
    }

    private fun RtmpPacket.typeName(): String {
        return when (typeId) {
            RtmpConstants.MSG_AUDIO -> "audio"
            RtmpConstants.MSG_VIDEO -> "video"
            RtmpConstants.MSG_DATA_AMF0, RtmpConstants.MSG_DATA_AMF3 -> "metadata"
            else -> "type_$typeId"
        }
    }

    private fun RtmpPacket.isRelayableMedia(): Boolean {
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
