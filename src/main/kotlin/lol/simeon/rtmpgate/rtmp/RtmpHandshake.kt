package lol.simeon.rtmpgate.rtmp

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import java.security.SecureRandom

object RtmpHandshake {
    const val HANDSHAKE_SIZE = 1536
    private val random = SecureRandom()

    fun canReadC0C1(input: ByteBuf): Boolean = input.readableBytes() >= 1 + HANDSHAKE_SIZE

    fun canReadC2(input: ByteBuf): Boolean = input.readableBytes() >= HANDSHAKE_SIZE

    fun handleServerHandshake(ctx: ChannelHandlerContext, input: ByteBuf) {
        val version = input.readUnsignedByte().toInt()
        require(version == 3) { "Unsupported RTMP version: $version" }
        val c1 = input.readRetainedSlice(HANDSHAKE_SIZE)
        val output = Unpooled.buffer(1 + HANDSHAKE_SIZE + HANDSHAKE_SIZE)
        output.writeByte(3)
        output.writeBytes(randomBytes(HANDSHAKE_SIZE))
        output.writeBytes(c1)
        c1.release()
        ctx.writeAndFlush(output)
    }

    fun discardC2(input: ByteBuf) {
        input.skipBytes(HANDSHAKE_SIZE)
    }

    fun clientC0C1(): ByteArray {
        val output = ByteArray(1 + HANDSHAKE_SIZE)
        output[0] = 3
        random.nextBytes(output, 1, HANDSHAKE_SIZE)
        return output
    }

    private fun randomBytes(size: Int): ByteArray {
        val bytes = ByteArray(size)
        random.nextBytes(bytes)
        return bytes
    }

    private fun SecureRandom.nextBytes(bytes: ByteArray, offset: Int, length: Int) {
        val tmp = ByteArray(length)
        nextBytes(tmp)
        tmp.copyInto(bytes, offset)
    }
}
