package lol.simeon.rtmpgate.rtmp

import lol.simeon.rtmpgate.metrics.RtmpGateMetrics
import java.nio.charset.StandardCharsets

/**
 * Keeps the latest stream metadata and codec sequence headers seen from the encoder so they
 * can be replayed to a freshly reconnected upstream, which has never seen them.
 *
 * Not thread-safe: owned by RtmpSession and confined to its channel event loop. Holds private
 * copies of the payloads, so callers keep releasing the packets they pass in as usual.
 */
class RtmpStreamHeaderCache {
    private var metadata: RtmpPacket? = null
    private var videoSequenceHeader: RtmpPacket? = null
    private var audioSequenceHeader: RtmpPacket? = null

    fun observe(packet: RtmpPacket) {
        when {
            RtmpMediaInspector.isMetadata(packet) -> metadata = replace(metadata, packet)
            RtmpMediaInspector.isVideoSequenceHeader(packet) -> videoSequenceHeader = replace(videoSequenceHeader, packet)
            RtmpMediaInspector.isAudioSequenceHeader(packet) -> audioSequenceHeader = replace(audioSequenceHeader, packet)
        }
    }

    /**
     * Fresh copies of the cached packets in replay order (metadata, video, audio), restamped to
     * [timestamp] so the upstream sees them at the current point of the stream rather than at
     * the original publish start. The caller owns the returned packets and must release them.
     */
    fun replayPackets(timestamp: Int): List<RtmpPacket> {
        return listOfNotNull(metadata, videoSequenceHeader, audioSequenceHeader).map { cached ->
            cached.copy(timestamp = timestamp, payload = cached.payload.copy())
        }
    }

    fun release() {
        metadata?.release()
        videoSequenceHeader?.release()
        audioSequenceHeader?.release()
        metadata = null
        videoSequenceHeader = null
        audioSequenceHeader = null
    }

    private fun replace(previous: RtmpPacket?, next: RtmpPacket): RtmpPacket {
        previous?.release()
        return next.copy(payload = next.payload.copy())
    }
}

/** Reads FLV tag headers from relayed RTMP media payloads without consuming them. */
object RtmpMediaInspector {
    private const val FRAME_TYPE_KEY = 1
    private const val CODEC_AVC = 7
    private const val CODEC_HEVC_LEGACY = 12
    private const val SOUND_FORMAT_AAC = 10
    private const val PACKET_TYPE_SEQUENCE_HEADER = 0
    private const val EX_HEADER_FLAG = 0x80
    private const val AMF0_STRING = 0x02
    private val METADATA_NAMES = setOf("@setDataFrame", "onMetaData")

    /** Video keyframe, legacy or Enhanced RTMP header (frame type lives in bits 4-6 for both). */
    fun isKeyframe(packet: RtmpPacket): Boolean {
        if (packet.typeId != RtmpConstants.MSG_VIDEO || packet.payloadSize < 1) return false
        return (byteAt(packet, 0) shr 4) and 0x07 == FRAME_TYPE_KEY
    }

    /** AVC (or legacy-HEVC) sequence header, or an Enhanced RTMP SequenceStart packet. */
    fun isVideoSequenceHeader(packet: RtmpPacket): Boolean {
        if (packet.typeId != RtmpConstants.MSG_VIDEO || packet.payloadSize < 2) return false
        val first = byteAt(packet, 0)
        if (first and EX_HEADER_FLAG != 0) return first and 0x0f == PACKET_TYPE_SEQUENCE_HEADER
        val codecId = first and 0x0f
        return (codecId == CODEC_AVC || codecId == CODEC_HEVC_LEGACY) && byteAt(packet, 1) == PACKET_TYPE_SEQUENCE_HEADER
    }

    /** AAC AudioSpecificConfig (AACPacketType 0). */
    fun isAudioSequenceHeader(packet: RtmpPacket): Boolean {
        if (packet.typeId != RtmpConstants.MSG_AUDIO || packet.payloadSize < 2) return false
        return byteAt(packet, 0) shr 4 == SOUND_FORMAT_AAC && byteAt(packet, 1) == PACKET_TYPE_SEQUENCE_HEADER
    }

    /** A data message whose first AMF0 value is "@setDataFrame" or "onMetaData". */
    fun isMetadata(packet: RtmpPacket): Boolean {
        val offset = when (packet.typeId) {
            RtmpConstants.MSG_DATA_AMF0 -> 0
            // AMF3 data messages carry one format byte before the AMF0 body.
            RtmpConstants.MSG_DATA_AMF3 -> 1
            else -> return false
        }
        return firstAmf0String(packet, offset) in METADATA_NAMES
    }

    private fun firstAmf0String(packet: RtmpPacket, offset: Int): String? {
        if (packet.payloadSize < offset + 3 || byteAt(packet, offset) != AMF0_STRING) return null
        val length = (byteAt(packet, offset + 1) shl 8) or byteAt(packet, offset + 2)
        if (packet.payloadSize < offset + 3 + length) return null
        val bytes = ByteArray(length)
        packet.payload.getBytes(packet.payload.readerIndex() + offset + 3, bytes)
        return String(bytes, StandardCharsets.UTF_8)
    }

    private fun byteAt(packet: RtmpPacket, index: Int): Int {
        return packet.payload.getUnsignedByte(packet.payload.readerIndex() + index).toInt()
    }
}

/**
 * What a session needs to resume relaying on a new upstream connection: the cached stream
 * headers, the stream's current timestamp, and a gate that holds video back until the next
 * keyframe. Not thread-safe: confined to the session's event loop.
 */
class RtmpRelayResume {
    private val headers = RtmpStreamHeaderCache()
    private var lastMediaTimestamp = 0
    private var awaitingKeyframe = false

    /** Call for every relayable packet the encoder sends, relayed or dropped. */
    fun observe(packet: RtmpPacket) {
        headers.observe(packet)
        if (packet.typeId == RtmpConstants.MSG_AUDIO || packet.typeId == RtmpConstants.MSG_VIDEO) {
            lastMediaTimestamp = maxOf(lastMediaTimestamp, packet.timestamp)
        }
    }

    /**
     * Starts resuming on a new upstream: returns the cached headers to send first, stamped at
     * the stream's current time (the caller releases them), and holds video until a keyframe.
     */
    fun beginResume(): List<RtmpPacket> {
        awaitingKeyframe = true
        return headers.replayPackets(lastMediaTimestamp)
    }

    /**
     * True if [packet] may go to the upstream. While resuming, video inter frames are dropped:
     * they reference pictures the new upstream never received. Audio, metadata and new
     * sequence headers pass; the first keyframe ends the hold.
     */
    fun admit(packet: RtmpPacket): Boolean {
        if (!awaitingKeyframe || packet.typeId != RtmpConstants.MSG_VIDEO) return true
        if (RtmpMediaInspector.isVideoSequenceHeader(packet)) return true
        if (RtmpMediaInspector.isKeyframe(packet)) {
            awaitingKeyframe = false
            return true
        }
        RtmpGateMetrics.keyframeWaitDroppedMessage()
        return false
    }

    fun release() {
        headers.release()
    }
}
