package lol.simeon.rtmpgate.rtmp

import lol.simeon.rtmpgate.config.AppConfig
import lol.simeon.rtmpgate.metrics.RtmpGateMetrics
import io.netty.buffer.Unpooled
import org.slf4j.LoggerFactory
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/** An upstream RTMP failure. The message carries the upstream's status code and description where known. */
class RtmpUpstreamException(message: String, cause: Throwable? = null) : IOException(message, cause)

class RtmpUpstreamClient(
    targetUrl: String,
    sourceApp: String,
    sourceStreamKey: String,
    private val config: AppConfig,
) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(RtmpUpstreamClient::class.java)
    private val target = RtmpTarget.parse(targetUrl, sourceApp, sourceStreamKey)
    private val socket = Socket()
    private val codec = RtmpChunkCodec(maxMessageBytes = config.maxRtmpMessageBytes)

    // Inbound (upstream -> rtmpgate) parsing has its own codec and buffer. After publish they are
    // used by the status reader thread while close() may run on another thread, so the buffer
    // is guarded by readLock.
    private val inboundCodec = RtmpChunkCodec(maxMessageBytes = config.maxRtmpMessageBytes)
    private val readBuffer = Unpooled.buffer()
    private val readLock = Any()
    private lateinit var input: BufferedInputStream
    private lateinit var output: BufferedOutputStream
    private var upstreamStreamId = 1

    // Media frames are handed off to a dedicated writer thread so the Netty event loop never
    // blocks on upstream socket I/O. The queue is drained FIFO; `queuedBytes` tracks real
    // outstanding bytes so backpressure (auto-read pause in RtmpSession) reflects reality.
    private val writeQueue = LinkedBlockingQueue<ByteArray>()
    private val queuedBytes = AtomicLong()
    @Volatile
    private var backpressureSince: Long? = null
    @Volatile
    private var running = false
    private val failure = AtomicReference<Throwable?>()
    private var writerThread: Thread? = null

    /**
     * Connects, handshakes and publishes. With publish confirmation on (the default) this only
     * returns once the upstream answers onStatus NetStream.Publish.Start; an error onStatus,
     * `_error`, upstream close or timeout throws [RtmpUpstreamException] (or the socket's own
     * IOException). Blocking: never call it on the event loop. The caller owns the client and
     * must [close] it when this throws.
     */
    fun connectAndPublish() {
        logger.info(
            "Connecting upstream target={} host={} port={} app={} streamKey={}",
            target.tcUrl, target.host, target.port, target.app, target.streamKey,
        )
        socket.soTimeout = config.readTimeoutMillis
        socket.tcpNoDelay = true
        socket.keepAlive = true
        socket.sendBufferSize = config.upstreamHighWatermarkBytes.coerceAtLeast(64 * 1024)
        socket.connect(InetSocketAddress(target.host, target.port), config.connectTimeoutMillis)
        input = BufferedInputStream(socket.getInputStream())
        output = BufferedOutputStream(socket.getOutputStream())

        doHandshake()
        writeSync(RtmpClientRequests.connect(target.app, target.tcUrl), chunkStreamId = 3)
        waitForCommand("_result", 1.0)

        writeSync(RtmpClientRequests.releaseStream(target.streamKey), chunkStreamId = 3)
        writeSync(RtmpClientRequests.fcPublish(target.streamKey), chunkStreamId = 3)
        writeSync(RtmpClientRequests.createStream(), chunkStreamId = 3)
        waitForCommand("_result", 4.0)

        writeSync(RtmpClientRequests.publish(upstreamStreamId, target.streamKey), chunkStreamId = 5)
        if (config.waitForPublishStart) {
            val status = awaitPublishStart()
            logger.info(
                "Upstream publish confirmed target=rtmp://{}:{}/{}/{} code={} description={}",
                target.host, target.port, target.app, target.streamKey, status.code, status.description,
            )
        } else {
            logger.info(
                "Upstream publish started without confirmation target=rtmp://{}:{}/{}/{}",
                target.host, target.port, target.app, target.streamKey,
            )
        }

        running = true
        writerThread = Thread({ writerLoop() }, "rtmp-upstream-writer-${target.streamKey}").apply {
            isDaemon = true
            start()
        }
        // The status reader belongs to publish confirmation: with RTMPGATE_WAIT_FOR_PUBLISH_START
        // off, the client behaves as before and never reads the upstream after publish.
        if (config.waitForPublishStart) {
            // A publishing upstream may stay silent indefinitely, so the reader blocks without a
            // timeout; close() unblocks it by closing the socket.
            socket.soTimeout = 0
            Thread({ readerLoop() }, "rtmp-upstream-reader-${target.streamKey}").apply {
                isDaemon = true
                start()
            }
        }
    }

    /**
     * Enqueues a media frame for the writer thread. Runs on the Netty event loop: it only
     * encodes (CPU) and hands the bytes off — it never touches the socket, so a stalled upstream
     * cannot block the event loop. Returns false and queues nothing once the upstream has
     * failed; the caller reads the reason from [failure].
     */
    fun writeMedia(packet: RtmpPacket): Boolean {
        if (failure.get() != null) return false

        val rewritten = packet.retainedCopyWithStreamId(upstreamStreamId)
        val chunkStreamId = when (packet.typeId) {
            RtmpConstants.MSG_AUDIO -> 4
            RtmpConstants.MSG_VIDEO -> 6
            RtmpConstants.MSG_DATA_AMF0, RtmpConstants.MSG_DATA_AMF3 -> 5
            else -> 5
        }

        val bytes = try {
            encodeToBytes(codec.encodePacket(rewritten, chunkStreamId, RtmpConstants.DEFAULT_CHUNK_SIZE))
        } finally {
            rewritten.release()
        }

        val queued = queuedBytes.addAndGet(bytes.size.toLong())
        RtmpGateMetrics.relayWriteQueueBytes(queued)
        if (queued >= config.upstreamHighWatermarkBytes && backpressureSince == null) {
            backpressureSince = System.currentTimeMillis()
            RtmpGateMetrics.backpressureEvent()
        }
        writeQueue.put(bytes)
        return true
    }

    fun isBackpressured(): Boolean = queuedBytes.get() >= config.upstreamHighWatermarkBytes

    fun queueBytes(): Long = queuedBytes.get()

    /** Non-null once the writer or the status reader has hit an unrecoverable upstream error. */
    fun failure(): Throwable? = failure.get()

    fun backpressureDurationMillis(now: Long = System.currentTimeMillis()): Long {
        val since = backpressureSince ?: return 0
        return now - since
    }

    override fun close() {
        running = false
        writerThread?.interrupt()
        // Closing the socket also unblocks the status reader or an in-flight connectAndPublish.
        runCatching { socket.close() }
        synchronized(readLock) {
            if (readBuffer.refCnt() > 0) readBuffer.release()
        }
    }

    private fun writerLoop() {
        try {
            while (running) {
                val bytes = writeQueue.poll(WRITER_POLL_MILLIS, TimeUnit.MILLISECONDS) ?: continue
                output.write(bytes)
                output.flush()

                val queued = (queuedBytes.addAndGet(-bytes.size.toLong())).coerceAtLeast(0)
                RtmpGateMetrics.relayWriteQueueBytes(queued)
                if (queued <= config.upstreamLowWatermarkBytes) {
                    backpressureSince = null
                }
            }
        } catch (interrupted: InterruptedException) {
            Thread.currentThread().interrupt()
        } catch (error: Throwable) {
            recordFailure("writer", error)
        }
    }

    /**
     * Drains what the upstream sends after a confirmed publish. Without it an upstream that
     * closes, resets or sends an error onStatus goes unnoticed until a write fails.
     */
    private fun readerLoop() {
        val tmp = ByteArray(READ_CHUNK_BYTES)
        try {
            while (running) {
                val read = input.read(tmp)
                if (read < 0) {
                    recordFailure("reader", RtmpUpstreamException("Upstream closed the RTMP connection"))
                    return
                }
                val rejection = appendAndParse(tmp, read)
                    .mapNotNull(inboundCodec::command)
                    .onEach(::debugCommand)
                    .firstOrNull(::isFatalStatus)
                if (rejection != null) {
                    recordFailure("reader", RtmpUpstreamException("Upstream sent ${rejection.statusSummary()}"))
                    return
                }
            }
        } catch (error: Throwable) {
            recordFailure("reader", error)
        }
    }

    /** Keeps the first failure only, so the reason is counted and logged once per client. */
    private fun recordFailure(source: String, error: Throwable) {
        if (!running) return
        if (!failure.compareAndSet(null, error)) return
        RtmpGateMetrics.upstreamFailure()
        logger.warn(
            "Upstream {} failed target={} streamKey={} error={}",
            source, target.tcUrl, target.streamKey, error.message,
        )
    }

    private fun doHandshake() {
        output.write(RtmpHandshake.clientC0C1())
        output.flush()

        val s0s1s2 = input.readExactly(1 + RtmpHandshake.HANDSHAKE_SIZE + RtmpHandshake.HANDSHAKE_SIZE)
        require(s0s1s2[0].toInt() == 3) { "Unsupported upstream RTMP version: ${s0s1s2[0].toInt()}" }
        val s1 = s0s1s2.copyOfRange(1, 1 + RtmpHandshake.HANDSHAKE_SIZE)
        output.write(s1)
        output.flush()
    }

    private fun waitForCommand(name: String, transactionId: Double) {
        val deadline = System.currentTimeMillis() + config.readTimeoutMillis
        while (System.currentTimeMillis() < deadline) {
            val messages = readSomeMessages()
            for (message in messages) {
                val command = inboundCodec.command(message) ?: continue
                debugCommand(command)
                // An error onStatus (or a fatal _error) is a rejection, never a reply.
                if (isFatalStatus(command)) {
                    throw RtmpUpstreamException("Upstream rejected $name/$transactionId: ${command.statusSummary()}")
                }
                if (command.name == name && command.transactionId == transactionId) return
                if (command.name == "onStatus") return
            }
        }
        error("Timed out waiting for upstream RTMP command $name/$transactionId")
    }

    /** Reads until onStatus NetStream.Publish.Start, bounded by RTMPGATE_PUBLISH_CONFIRM_TIMEOUT_MS. */
    private fun awaitPublishStart(): RtmpCommand {
        val timeoutMillis = config.publishConfirmTimeoutMillis.coerceAtLeast(1)
        val deadline = System.currentTimeMillis() + timeoutMillis
        val previousSoTimeout = socket.soTimeout
        return try {
            readUntilPublishStart(deadline) ?: throw publishConfirmTimeout(timeoutMillis, cause = null)
        } catch (timeout: SocketTimeoutException) {
            throw publishConfirmTimeout(timeoutMillis, timeout)
        } catch (error: RtmpUpstreamException) {
            throw error
        } catch (error: IOException) {
            throw RtmpUpstreamException(
                "Upstream connection failed while waiting for $PUBLISH_START_CODE: ${error.message}",
                error,
            )
        } finally {
            if (!socket.isClosed) socket.soTimeout = previousSoTimeout
        }
    }

    /** Returns the Publish.Start status, or null once [deadline] passes. */
    private fun readUntilPublishStart(deadline: Long): RtmpCommand? {
        while (true) {
            val remaining = deadline - System.currentTimeMillis()
            if (remaining <= 0) return null
            socket.soTimeout = remaining.toInt().coerceAtLeast(1)

            for (command in readSomeMessages().mapNotNull(inboundCodec::command)) {
                debugCommand(command)
                if (isFatalStatus(command)) {
                    throw RtmpUpstreamException("Upstream rejected publish: ${command.statusSummary()}")
                }
                if (command.name == "onStatus" && command.code == PUBLISH_START_CODE) return command
            }
        }
    }

    private fun publishConfirmTimeout(timeoutMillis: Int, cause: Throwable?): RtmpUpstreamException {
        return RtmpUpstreamException("Timed out after ${timeoutMillis}ms waiting for upstream $PUBLISH_START_CODE", cause)
    }

    /**
     * An error onStatus is always fatal. An `_error` is fatal except for releaseStream/FCPublish,
     * which several servers answer with `_error` even though the publish then succeeds.
     */
    private fun isFatalStatus(command: RtmpCommand): Boolean {
        if (!command.isErrorStatus()) return false
        if (command.name == "_error" && command.transactionId in ADVISORY_TRANSACTION_IDS) {
            logger.info("Ignoring upstream _error for advisory command target={} {}", target.tcUrl, command.statusSummary())
            return false
        }
        return true
    }

    private fun debugCommand(command: RtmpCommand) {
        if (config.rtmpDebug) {
            logger.info(
                "Upstream command name={} transactionId={} code={} level={}",
                command.name, command.transactionId, command.code, command.level,
            )
        }
    }

    private fun readSomeMessages(): List<RtmpMessage> {
        val tmp = ByteArray(READ_CHUNK_BYTES)
        val read = input.read(tmp)
        if (read < 0) throw RtmpUpstreamException("Upstream closed the RTMP connection")
        return appendAndParse(tmp, read)
    }

    private fun appendAndParse(bytes: ByteArray, length: Int): List<RtmpMessage> {
        synchronized(readLock) {
            if (readBuffer.refCnt() == 0) throw RtmpUpstreamException("Upstream client closed")
            readBuffer.writeBytes(bytes, 0, length)
            val messages = inboundCodec.readMessages(readBuffer)
            readBuffer.discardReadBytes()
            return messages
        }
    }

    /** Synchronous write used only during the connect/publish handshake (before the writer thread starts). */
    private fun writeSync(message: RtmpMessage, chunkStreamId: Int) {
        val encoded = codec.encode(
            message,
            chunkStreamId = chunkStreamId,
            outboundChunkSize = RtmpConstants.DEFAULT_CHUNK_SIZE,
        )
        val bytes = encodeToBytes(encoded)
        output.write(bytes)
        output.flush()
    }

    private fun encodeToBytes(encoded: io.netty.buffer.ByteBuf): ByteArray {
        val bytes = ByteArray(encoded.readableBytes())
        encoded.readBytes(bytes)
        encoded.release()
        return bytes
    }

    private fun BufferedInputStream.readExactly(size: Int): ByteArray {
        val result = ByteArray(size)
        var offset = 0
        while (offset < size) {
            val read = read(result, offset, size - offset)
            if (read < 0) error("Unexpected end of stream during RTMP handshake")
            offset += read
        }
        return result
    }

    private companion object {
        const val WRITER_POLL_MILLIS = 200L
        const val READ_CHUNK_BYTES = 4096
        const val PUBLISH_START_CODE = "NetStream.Publish.Start"

        // releaseStream (2) and FCPublish (3) in RtmpClientRequests.
        val ADVISORY_TRANSACTION_IDS = setOf(2.0, 3.0)
    }
}
