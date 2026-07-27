package lol.simeon.rtmpgate.rtmp

import lol.simeon.rtmpgate.config.AppConfig
import lol.simeon.rtmpgate.metrics.RtmpGateMetrics
import io.netty.buffer.Unpooled
import org.slf4j.LoggerFactory
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

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
    private val readBuffer = Unpooled.buffer()
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
    @Volatile
    private var failure: Throwable? = null
    private var writerThread: Thread? = null

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
        logger.info("Upstream publish started target=rtmp://{}:{}/{}/{}", target.host, target.port, target.app, target.streamKey)

        running = true
        writerThread = Thread({ writerLoop() }, "rtmp-upstream-writer-${target.streamKey}").apply {
            isDaemon = true
            start()
        }
    }

    /**
     * Enqueues a media frame for the writer thread. Runs on the Netty event loop: it only
     * encodes (CPU) and hands the bytes off — it never touches the socket, so a stalled upstream
     * cannot block the event loop. Throws if the writer thread has already failed so the caller
     * can tear the session down.
     */
    fun writeMedia(packet: RtmpPacket) {
        failure?.let { throw it }

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
    }

    fun isBackpressured(): Boolean = queuedBytes.get() >= config.upstreamHighWatermarkBytes

    fun queueBytes(): Long = queuedBytes.get()

    /** Non-null once the writer thread has hit an unrecoverable I/O error. */
    fun failure(): Throwable? = failure

    fun backpressureDurationMillis(now: Long = System.currentTimeMillis()): Long {
        val since = backpressureSince ?: return 0
        return now - since
    }

    override fun close() {
        running = false
        writerThread?.interrupt()
        runCatching { readBuffer.release() }
        runCatching { socket.close() }
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
            if (running) {
                failure = error
                RtmpGateMetrics.upstreamFailure()
                logger.warn(
                    "Upstream writer failed target={} streamKey={} error={}",
                    target.tcUrl, target.streamKey, error.message,
                )
            }
        }
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
                val command = codec.command(message) ?: continue
                if (config.rtmpDebug) {
                    logger.info("Upstream command name={} transactionId={}", command.name, command.transactionId)
                }
                if (command.name == name && command.transactionId == transactionId) return
                if (command.name == "onStatus") return
            }
        }
        error("Timed out waiting for upstream RTMP command $name/$transactionId")
    }

    private fun readSomeMessages(): List<RtmpMessage> {
        val tmp = ByteArray(4096)
        val read = input.read(tmp)
        if (read < 0) error("Upstream closed the RTMP connection")
        readBuffer.writeBytes(tmp, 0, read)
        return codec.readMessages(readBuffer)
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
    }
}
