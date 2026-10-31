package lol.simeon.rtmpgate.rtmp

data class RtmpCommand(
    val name: String,
    val transactionId: Double,
    val app: String? = null,
    val streamKey: String? = null,
    // Info-object fields, populated for onStatus and _error commands.
    val level: String? = null,
    val code: String? = null,
    val description: String? = null,
) {
    /**
     * True for an onStatus/_error that reports a failure. RTMP servers signal errors with
     * level=error; a few only put it in the code (e.g. NetStream.Publish.BadName), so both count.
     */
    fun isErrorStatus(): Boolean {
        if (name == "_error") return true
        if (name != "onStatus") return false
        if (level.equals("error", ignoreCase = true)) return true
        val statusCode = code.orEmpty()
        return ERROR_CODE_MARKERS.any { statusCode.contains(it, ignoreCase = true) }
    }

    fun statusSummary(): String = "name=$name code=${code ?: "-"} level=${level ?: "-"} description=${description ?: "-"}"

    private companion object {
        val ERROR_CODE_MARKERS = listOf("BadName", "Failed", "Rejected")
    }
}
