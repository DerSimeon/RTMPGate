package lol.simeon.rtmpgate.rtmp

enum class RtmpSessionState {
    WAIT_C0_C1,
    WAIT_C2,
    WAIT_PUBLISH,
    STARTING_UPSTREAM,
    RELAYING,
    // Upstream failed while relaying: the encoder stays connected, its media is dropped
    // and the upstream is reconnected in the background.
    UPSTREAM_RETRYING,
    CLOSED,
}
