package lol.simeon.rtmpgate.rtmp

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.thread

/** How a [FakeRtmpUpstream] connection answers `publish`. */
sealed interface PublishBehavior {
    /** onStatus NetStream.Publish.Start, then keep the connection open. */
    data object Accept : PublishBehavior

    /** onStatus NetStream.Publish.Start, then a TCP reset after [afterMillis]. */
    data class AcceptThenReset(val afterMillis: Long) : PublishBehavior

    /** An error-level onStatus, then close. */
    data class RejectWithStatus(val code: String, val description: String) : PublishBehavior

    /** A `_error` reply to publish, then close. */
    data object ErrorCommand : PublishBehavior

    /** No reply at all; the connection stays open. */
    data object Silent : PublishBehavior

    /** Close the connection without replying. */
    data object CloseAfterPublish : PublishBehavior
}

const val GOOGLE_DUPLICATE_INPUT =
    "Another input stream with the same stream ID has already been accepted."

/**
 * A minimal RTMP ingest server for tests. Each accepted connection is numbered from 0 and asks
 * [behaviorFor] how to answer its publish. Media it receives is recorded per connection.
 */
class FakeRtmpUpstream(
    private val rejectConnect: Boolean = false,
    private val releaseStreamError: Boolean = false,
    private val behaviorFor: (Int) -> PublishBehavior,
) : AutoCloseable {
    private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
    private val sockets = CopyOnWriteArrayList<Socket>()
    val connections = CopyOnWriteArrayList<Connection>()
    val port: Int get() = server.localPort

    class Connection(val index: Int) {
        val media = CopyOnWriteArrayList<RtmpMessage>()

        @Volatile
        var closed = false
    }

    init {
        thread(isDaemon = true, name = "fake-upstream-accept") {
            while (!server.isClosed) {
                val socket = runCatching { server.accept() }.getOrNull() ?: break
                sockets += socket
                val connection = Connection(connections.size).also { connections += it }
                thread(isDaemon = true, name = "fake-upstream-${connection.index}") {
                    runCatching { serve(socket, connection) }
                    connection.closed = true
                    runCatching { socket.close() }
                }
            }
        }
    }

    fun targetUrl(streamKey: String): String = "rtmp://127.0.0.1:$port/live/$streamKey"

    override fun close() {
        runCatching { server.close() }
        sockets.forEach { runCatching { it.close() } }
    }

    private fun serve(socket: Socket, connection: Connection) {
        val input = socket.getInputStream()
        val output = socket.getOutputStream()
        input.readFully(1 + RtmpHandshake.HANDSHAKE_SIZE)
        output.write(ByteArray(1 + 2 * RtmpHandshake.HANDSHAKE_SIZE).also { it[0] = 3 })
        output.flush()
        input.readFully(RtmpHandshake.HANDSHAKE_SIZE)

        val codec = RtmpChunkCodec()
        val buffer = Unpooled.buffer()
        val chunk = ByteArray(8192)
        while (true) {
            val read = input.read(chunk)
            if (read < 0) return
            buffer.writeBytes(chunk, 0, read)
            for (message in codec.readMessages(buffer)) {
                val command = codec.command(message)
                if (command == null) {
                    if (message.typeId in MEDIA_TYPES) connection.media += message
                } else if (!handleCommand(socket, output, command, connection)) {
                    return
                }
            }
            buffer.discardReadBytes()
        }
    }

    /** Returns false when the connection should end. */
    private fun handleCommand(socket: Socket, output: OutputStream, command: RtmpCommand, connection: Connection): Boolean {
        when (command.name) {
            "connect" -> if (rejectConnect) {
                output.send(status("error", "NetConnection.Connect.Rejected", "Connect rejected by test"))
                return false
            } else {
                output.send(RtmpServerResponses.connectResult(command.transactionId))
            }

            "releaseStream" -> if (releaseStreamError) {
                output.send(errorCommand(command.transactionId, "NetStream.Release.Failed", "No such stream"))
            }

            "createStream" -> output.send(RtmpServerResponses.createStreamResult(command.transactionId, 1))
            "publish" -> return answerPublish(socket, output, behaviorFor(connection.index))
        }
        return true
    }

    private fun answerPublish(socket: Socket, output: OutputStream, behavior: PublishBehavior): Boolean {
        when (behavior) {
            PublishBehavior.Accept -> output.send(RtmpServerResponses.publishStart(1, "key"))
            is PublishBehavior.AcceptThenReset -> {
                output.send(RtmpServerResponses.publishStart(1, "key"))
                thread(isDaemon = true) {
                    Thread.sleep(behavior.afterMillis)
                    socket.setSoLinger(true, 0)
                    socket.close()
                }
            }

            is PublishBehavior.RejectWithStatus -> {
                output.send(status("error", behavior.code, behavior.description))
                return false
            }

            PublishBehavior.ErrorCommand -> {
                output.send(errorCommand(0.0, "NetStream.Publish.Denied", "Publish denied by test"))
                return false
            }

            PublishBehavior.Silent -> Unit
            PublishBehavior.CloseAfterPublish -> return false
        }
        return true
    }

    private fun status(level: String, code: String, description: String): ByteBuf {
        val payload = RtmpAmf0.concat(
            RtmpAmf0.string("onStatus"),
            RtmpAmf0.number(0.0),
            RtmpAmf0.nullValue(),
            RtmpAmf0.obj(
                "level" to RtmpAmf0.string(level),
                "code" to RtmpAmf0.string(code),
                "description" to RtmpAmf0.string(description),
            ),
        )
        return RtmpChunkCodec().encode(RtmpMessage(0, RtmpConstants.MSG_COMMAND_AMF0, 1, payload), chunkStreamId = 5)
    }

    private fun errorCommand(transactionId: Double, code: String, description: String): ByteBuf {
        val payload = RtmpAmf0.concat(
            RtmpAmf0.string("_error"),
            RtmpAmf0.number(transactionId),
            RtmpAmf0.nullValue(),
            RtmpAmf0.obj(
                "level" to RtmpAmf0.string("error"),
                "code" to RtmpAmf0.string(code),
                "description" to RtmpAmf0.string(description),
            ),
        )
        return RtmpChunkCodec().encode(RtmpMessage(0, RtmpConstants.MSG_COMMAND_AMF0, 0, payload), chunkStreamId = 3)
    }

    private companion object {
        val MEDIA_TYPES = setOf(
            RtmpConstants.MSG_AUDIO,
            RtmpConstants.MSG_VIDEO,
            RtmpConstants.MSG_DATA_AMF0,
            RtmpConstants.MSG_DATA_AMF3,
        )
    }
}

/**
 * A raw RTMP publisher standing in for an encoder. [send] writes all given messages with one
 * socket write so tests can put several packets into one inbound read on the gateway.
 */
class TestRtmpEncoder(port: Int, private val streamKey: String) : AutoCloseable {
    private val socket = Socket()
    private val codec = RtmpChunkCodec()
    private val input: InputStream
    private val output: OutputStream

    @Volatile
    private var streaming = false
    private var streamer: Thread? = null

    init {
        socket.connect(InetSocketAddress("127.0.0.1", port), 2_000)
        socket.soTimeout = 5_000
        input = socket.getInputStream()
        output = socket.getOutputStream()
    }

    /** Handshake, connect, createStream and publish; returns the gateway's publish onStatus. */
    fun publish(): RtmpCommand {
        output.write(RtmpHandshake.clientC0C1())
        output.flush()
        val s0s1s2 = input.readFully(1 + 2 * RtmpHandshake.HANDSHAKE_SIZE)
        output.write(s0s1s2, 1, RtmpHandshake.HANDSHAKE_SIZE)
        output.flush()

        sendCommand(RtmpClientRequests.connect("live", "rtmp://127.0.0.1/live"), chunkStreamId = 3)
        sendCommand(RtmpClientRequests.createStream(), chunkStreamId = 3)
        sendCommand(RtmpClientRequests.publish(1, streamKey), chunkStreamId = 5)

        val buffer = Unpooled.buffer()
        val chunk = ByteArray(4096)
        while (true) {
            val read = input.read(chunk)
            check(read >= 0) { "Gateway closed the connection before answering publish" }
            buffer.writeBytes(chunk, 0, read)
            codec.readMessages(buffer).mapNotNull(codec::command).firstOrNull { it.name == "onStatus" }?.let { return it }
        }
    }

    fun send(vararg messages: RtmpMessage) {
        val bytes = ByteArrayOutputStream()
        for (message in messages) {
            val encoded = codec.encode(message.copy(streamId = 1), chunkStreamIdFor(message))
            bytes.write(ByteArray(encoded.readableBytes()).also(encoded::readBytes))
            encoded.release()
        }
        output.write(bytes.toByteArray())
        output.flush()
    }

    /** Sends headers, then audio + video every [intervalMillis] with a keyframe every 10th video frame. */
    fun startStreaming(intervalMillis: Long = 20) {
        streaming = true
        send(TestMedia.metadata(0), TestMedia.avcSequenceHeader(0), TestMedia.aacSequenceHeader(0))
        streamer = thread(isDaemon = true, name = "test-encoder-$streamKey") {
            var frame = 0
            while (streaming) {
                val timestamp = frame * intervalMillis.toInt()
                val video = if (frame % 10 == 0) TestMedia.keyframe(timestamp) else TestMedia.interFrame(timestamp)
                if (runCatching { send(TestMedia.aacFrame(timestamp), video) }.isFailure) return@thread
                frame++
                Thread.sleep(intervalMillis)
            }
        }
    }

    /** True while the gateway keeps this connection open. */
    fun isOpen(): Boolean {
        val previousTimeout = runCatching { socket.soTimeout }.getOrDefault(5_000)
        val read = runCatching {
            socket.soTimeout = 50
            input.read(ByteArray(4096))
        }
        runCatching { socket.soTimeout = previousTimeout }
        // A read timeout means the connection is open but idle; EOF or a reset means closed.
        return read.fold(onSuccess = { it >= 0 }, onFailure = { it is SocketTimeoutException })
    }

    override fun close() {
        streaming = false
        streamer?.join(1_000)
        runCatching { socket.close() }
    }

    private fun sendCommand(message: RtmpMessage, chunkStreamId: Int) {
        val encoded = codec.encode(message, chunkStreamId)
        output.write(ByteArray(encoded.readableBytes()).also(encoded::readBytes))
        output.flush()
        encoded.release()
    }

    private fun chunkStreamIdFor(message: RtmpMessage): Int = when (message.typeId) {
        RtmpConstants.MSG_AUDIO -> 4
        RtmpConstants.MSG_VIDEO -> 6
        else -> 5
    }
}

/** FLV-shaped test payloads. Only the tag header bytes matter to the gateway. */
object TestMedia {
    fun metadata(timestamp: Int) = RtmpMessage(
        timestamp,
        RtmpConstants.MSG_DATA_AMF0,
        1,
        RtmpAmf0.concat(
            RtmpAmf0.string("@setDataFrame"),
            RtmpAmf0.string("onMetaData"),
            RtmpAmf0.obj("width" to RtmpAmf0.number(1280.0)),
        ),
    )

    fun avcSequenceHeader(timestamp: Int) = video(timestamp, 0x17, 0x00, 0x01, 0x64, 0x00, 0x1f)

    fun keyframe(timestamp: Int) = video(timestamp, 0x17, 0x01, 0x00, 0x00, 0x00, 0x65)

    fun interFrame(timestamp: Int) = video(timestamp, 0x27, 0x01, 0x00, 0x00, 0x00, 0x41)

    fun aacSequenceHeader(timestamp: Int) = audio(timestamp, 0xaf, 0x00, 0x12, 0x10)

    fun aacFrame(timestamp: Int) = audio(timestamp, 0xaf, 0x01, 0x21, 0x00)

    private fun video(timestamp: Int, vararg bytes: Int) =
        RtmpMessage(timestamp, RtmpConstants.MSG_VIDEO, 1, ByteArray(bytes.size) { bytes[it].toByte() })

    private fun audio(timestamp: Int, vararg bytes: Int) =
        RtmpMessage(timestamp, RtmpConstants.MSG_AUDIO, 1, ByteArray(bytes.size) { bytes[it].toByte() })
}

fun RtmpMessage.toPacket(): RtmpPacket = RtmpPacket.fromMessage(this)

private fun OutputStream.send(encoded: ByteBuf) {
    val bytes = ByteArray(encoded.readableBytes())
    encoded.readBytes(bytes)
    encoded.release()
    write(bytes)
    flush()
}

private fun InputStream.readFully(size: Int): ByteArray {
    val result = ByteArray(size)
    var offset = 0
    while (offset < size) {
        val read = read(result, offset, size - offset)
        if (read < 0) throw IOException("Unexpected end of stream")
        offset += read
    }
    return result
}
