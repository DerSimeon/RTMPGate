package lol.simeon.rtmpgate.rtmp

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

object RtmpAmf0 {
    private const val NUMBER = 0x00
    private const val BOOLEAN = 0x01
    private const val STRING = 0x02
    private const val OBJECT = 0x03
    private const val NULL = 0x05
    private const val UNDEFINED = 0x06
    private const val ECMA_ARRAY = 0x08
    private const val OBJECT_END = 0x09

    fun parseCommand(message: RtmpMessage): RtmpCommand? {
        if (message.typeId != RtmpConstants.MSG_COMMAND_AMF0 && message.typeId != RtmpConstants.MSG_COMMAND_AMF3) return null

        val input = Unpooled.wrappedBuffer(message.payload)
        return try {
            if (message.typeId == RtmpConstants.MSG_COMMAND_AMF3 && input.isReadable) {
                input.skipBytes(1)
            }

            val name = readString(input) ?: return null
            val transactionId = readNumber(input) ?: 0.0

            when (name) {
                "connect" -> RtmpCommand(
                    name = name,
                    transactionId = transactionId,
                    app = readConnectApp(input),
                )

                "publish", "releaseStream", "FCPublish", "FCUnpublish" -> {
                    skipValue(input)
                    RtmpCommand(
                        name = name,
                        transactionId = transactionId,
                        streamKey = readString(input),
                    )
                }

                else -> RtmpCommand(name = name, transactionId = transactionId)
            }
        } finally {
            input.release()
        }
    }

    fun string(value: String): ByteArray = concat(byteArrayOf(STRING.toByte()), stringValue(value))

    fun number(value: Double): ByteArray = concat(byteArrayOf(NUMBER.toByte()), numberValue(value))

    fun nullValue(): ByteArray = byteArrayOf(NULL.toByte())

    fun boolean(value: Boolean): ByteArray = byteArrayOf(BOOLEAN.toByte(), if (value) 1 else 0)

    fun obj(vararg properties: Pair<String, ByteArray>): ByteArray {
        val parts = mutableListOf<ByteArray>()
        parts += byteArrayOf(OBJECT.toByte())
        for ((key, value) in properties) {
            val keyBytes = key.toByteArray(StandardCharsets.UTF_8)
            parts += ushort(keyBytes.size)
            parts += keyBytes
            parts += value
        }
        parts += byteArrayOf(0x00, 0x00, OBJECT_END.toByte())
        return concat(*parts.toTypedArray())
    }

    fun concat(vararg parts: ByteArray): ByteArray {
        val out = ByteArray(parts.sumOf { it.size })
        var offset = 0
        for (part in parts) {
            part.copyInto(out, offset)
            offset += part.size
        }
        return out
    }

    private fun stringValue(value: String): ByteArray {
        val bytes = value.toByteArray(StandardCharsets.UTF_8)
        return concat(ushort(bytes.size), bytes)
    }

    private fun numberValue(value: Double): ByteArray = ByteBuffer.allocate(8).putDouble(value).array()

    private fun ushort(value: Int): ByteArray = byteArrayOf(
        ((value ushr 8) and 0xff).toByte(),
        (value and 0xff).toByte(),
    )

    private fun readString(input: ByteBuf): String? {
        if (!input.isReadable) return null
        val marker = input.readUnsignedByte().toInt()
        if (marker != STRING) return null
        if (input.readableBytes() < 2) return null
        val length = input.readUnsignedShort()
        if (input.readableBytes() < length) return null
        val bytes = ByteArray(length)
        input.readBytes(bytes)
        return String(bytes, StandardCharsets.UTF_8)
    }

    private fun readNumber(input: ByteBuf): Double? {
        if (!input.isReadable) return null
        val marker = input.readUnsignedByte().toInt()
        if (marker != NUMBER || input.readableBytes() < 8) return null
        return input.readDouble()
    }

    private fun readConnectApp(input: ByteBuf): String? {
        if (!input.isReadable) return null
        val marker = input.readUnsignedByte().toInt()
        if (marker != OBJECT && marker != ECMA_ARRAY) return null
        if (marker == ECMA_ARRAY && input.readableBytes() >= 4) input.skipBytes(4)

        while (input.readableBytes() >= 3) {
            val keyLength = input.readUnsignedShort()
            if (keyLength == 0) {
                val maybeEnd = input.readUnsignedByte().toInt()
                if (maybeEnd == OBJECT_END) return null
                input.readerIndex(input.readerIndex() - 1)
            }
            if (input.readableBytes() < keyLength) return null
            val keyBytes = ByteArray(keyLength)
            input.readBytes(keyBytes)
            val key = String(keyBytes, StandardCharsets.UTF_8)
            if (key == "app") return readString(input)
            skipValue(input)
        }
        return null
    }

    private fun skipValue(input: ByteBuf) {
        if (!input.isReadable) return

        when (val marker = input.readUnsignedByte().toInt()) {
            NUMBER -> input.skipIfReadable(8)
            BOOLEAN -> input.skipIfReadable(1)
            NULL, UNDEFINED -> Unit
            STRING -> skipString(input)
            OBJECT, ECMA_ARRAY -> skipObject(input, marker == ECMA_ARRAY)
            else -> Unit
        }
    }

    private fun skipString(input: ByteBuf) {
        if (input.readableBytes() < 2) return

        val length = input.readUnsignedShort()
        input.skipIfReadable(length)
    }

    private fun skipObject(input: ByteBuf, hasArrayLength: Boolean) {
        if (hasArrayLength && !input.skipIfReadable(4)) return

        while (input.readableBytes() >= 3) {
            if (isObjectEnd(input)) return

            val keyLength = input.readUnsignedShort()
            if (input.readableBytes() < keyLength) return

            input.skipBytes(keyLength)
            skipValue(input)
        }
    }

    private fun isObjectEnd(input: ByteBuf): Boolean {
        val keyLength = input.readUnsignedShort()
        if (keyLength != 0) {
            input.readerIndex(input.readerIndex() - 2)
            return false
        }

        val marker = input.readUnsignedByte().toInt()
        if (marker == OBJECT_END) return true

        input.readerIndex(input.readerIndex() - 3)
        return false
    }

    private fun ByteBuf.skipIfReadable(length: Int): Boolean {
        if (readableBytes() < length) return false

        skipBytes(length)
        return true
    }
}
