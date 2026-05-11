package lol.simeon.rtmpgate.rtmp

enum class RtmpSessionState {
    WAIT_C0_C1,
    WAIT_C2,
    WAIT_PUBLISH,
    STARTING_UPSTREAM,
    RELAYING,
    CLOSED,
}
