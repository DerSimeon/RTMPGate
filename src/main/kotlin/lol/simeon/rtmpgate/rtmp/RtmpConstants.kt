package lol.simeon.rtmpgate.rtmp

object RtmpConstants {
    const val DEFAULT_CHUNK_SIZE = 128
    const val DEFAULT_WINDOW_ACK_SIZE = 5_000_000

    const val MSG_SET_CHUNK_SIZE = 1
    const val MSG_ABORT = 2
    const val MSG_ACKNOWLEDGEMENT = 3
    const val MSG_USER_CONTROL = 4
    const val MSG_WINDOW_ACKNOWLEDGEMENT_SIZE = 5
    const val MSG_SET_PEER_BANDWIDTH = 6
    const val MSG_AUDIO = 8
    const val MSG_VIDEO = 9
    const val MSG_DATA_AMF0 = 18
    const val MSG_COMMAND_AMF0 = 20
    const val MSG_DATA_AMF3 = 15
    const val MSG_COMMAND_AMF3 = 17

    const val USER_CONTROL_STREAM_BEGIN = 0
}
