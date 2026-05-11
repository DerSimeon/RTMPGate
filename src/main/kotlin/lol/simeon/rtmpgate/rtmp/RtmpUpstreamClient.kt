package lol.simeon.rtmpgate.rtmp

import lol.simeon.rtmpgate.config.AppConfig
import io.netty.buffer.Unpooled
import org.slf4j.LoggerFactory
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.net.InetSocketAddress
import java.net.Socket

class RtmpUpstreamClient(
    targetUrl: String,
    sourceApp: String,
    sourceStreamKey: String,
    private val config: AppConfig,
) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(RtmpUpstreamClient::class.java)
    private val target = RtmpTarget.parse(targetUrl, sourceApp, sourceStreamKey)
    private val socket = Socket()
    private val codec = RtmpChunkCodec()
    private val readBuffer = Unpooled.buffer()
    private lateinit var input: BufferedInputStream
    private lateinit var output: BufferedOutputStream
    private var upstreamStreamId = 1
    private val writeLock = Any()

    fun connectAndPublish() {
        socket.soTimeout = config.readTimeoutMillis
        socket.tcpNoDelay = true
        socket.keepAlive = true
        socket.connect(InetSocketAddress(target.host, target.port), config.connectTimeoutMillis)
        input = BufferedInputStream(socket.getInputStream())
        output = BufferedOutputStream(socket.getOutputStream())

        doHandshake()
        write(RtmpClientRequests.connect(target.app, target.tcUrl), chunkStreamId = 3)
        waitForCommand("_result", 1.0)

        write(RtmpClientRequests.releaseStream(target.streamKey), chunkStreamId = 3)
        write(RtmpClientRequests.fcPublish(target.streamKey), chunkStreamId = 3)
        write(RtmpClientRequests.createStream(), chunkStreamId = 3)
        waitForCommand("_result", 4.0)

        write(RtmpClientRequests.publish(upstreamStreamId, target.streamKey), chunkStreamId = 5)
        logger.info("Upstream publish started target=rtmp://{}:{}/{}/{}", target.host, target.port, target.app, target.streamKey)
    }

    fun writeMedia(message: RtmpMessage) {
        val rewritten = message.copy(streamId = upstreamStreamId)
        val chunkStreamId = when (message.typeId) {
            RtmpConstants.MSG_AUDIO -> 4
            RtmpConstants.MSG_VIDEO -> 6
            RtmpConstants.MSG_DATA_AMF0, RtmpConstants.MSG_DATA_AMF3 -> 5
            else -> 5
        }
        write(rewritten, chunkStreamId)
    }

    override fun close() {
        runCatching { readBuffer.release() }
        runCatching { socket.close() }
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

    private fun write(message: RtmpMessage, chunkStreamId: Int) {
        // Keep upstream writes at RTMP's default 128-byte chunk size.
        // Do not use larger chunks unless this client first sends Set Chunk Size
        // and the upstream server has accepted it.
        val encoded = codec.encode(message, chunkStreamId = chunkStreamId, outboundChunkSize = RtmpConstants.DEFAULT_CHUNK_SIZE)
        val bytes = ByteArray(encoded.readableBytes())
        encoded.readBytes(bytes)
        encoded.release()
        synchronized(writeLock) {
            output.write(bytes)
            output.flush()
        }
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
}
