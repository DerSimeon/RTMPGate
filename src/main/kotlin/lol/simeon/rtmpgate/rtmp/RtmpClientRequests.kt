package lol.simeon.rtmpgate.rtmp

object RtmpClientRequests {
    fun connect(app: String, tcUrl: String): RtmpMessage {
        val payload = RtmpAmf0.concat(
            RtmpAmf0.string("connect"),
            RtmpAmf0.number(1.0),
            RtmpAmf0.obj(
                "app" to RtmpAmf0.string(app),
                "type" to RtmpAmf0.string("nonprivate"),
                "tcUrl" to RtmpAmf0.string(tcUrl),
                "flashVer" to RtmpAmf0.string("FMLE/3.0"),
                "fpad" to RtmpAmf0.boolean(false),
                "capabilities" to RtmpAmf0.number(15.0),
                "audioCodecs" to RtmpAmf0.number(4071.0),
                "videoCodecs" to RtmpAmf0.number(252.0),
                "videoFunction" to RtmpAmf0.number(1.0),
                "objectEncoding" to RtmpAmf0.number(0.0),
            ),
        )
        return RtmpMessage(0, RtmpConstants.MSG_COMMAND_AMF0, 0, payload)
    }

    fun releaseStream(streamKey: String): RtmpMessage {
        return command0("releaseStream", 2.0, streamKey)
    }

    fun fcPublish(streamKey: String): RtmpMessage {
        return command0("FCPublish", 3.0, streamKey)
    }

    fun createStream(): RtmpMessage {
        val payload = RtmpAmf0.concat(
            RtmpAmf0.string("createStream"),
            RtmpAmf0.number(4.0),
            RtmpAmf0.nullValue(),
        )
        return RtmpMessage(0, RtmpConstants.MSG_COMMAND_AMF0, 0, payload)
    }

    fun publish(streamId: Int, streamKey: String): RtmpMessage {
        val payload = RtmpAmf0.concat(
            RtmpAmf0.string("publish"),
            RtmpAmf0.number(0.0),
            RtmpAmf0.nullValue(),
            RtmpAmf0.string(streamKey),
            RtmpAmf0.string("live"),
        )
        return RtmpMessage(0, RtmpConstants.MSG_COMMAND_AMF0, streamId, payload)
    }

    private fun command0(name: String, transactionId: Double, streamKey: String): RtmpMessage {
        val payload = RtmpAmf0.concat(
            RtmpAmf0.string(name),
            RtmpAmf0.number(transactionId),
            RtmpAmf0.nullValue(),
            RtmpAmf0.string(streamKey),
        )
        return RtmpMessage(0, RtmpConstants.MSG_COMMAND_AMF0, 0, payload)
    }
}
