package lol.simeon.rtmpgate.routes

import com.github.benmanes.caffeine.cache.Caffeine
import java.time.Duration

class CachedRouteStore(
    private val delegate: RouteStore,
    ttlSeconds: Long,
) : RouteStore {
    private val cache = Caffeine.newBuilder()
        .expireAfterWrite(Duration.ofSeconds(ttlSeconds.coerceAtLeast(1)))
        .maximumSize(100_000)
        .build<String, RouteRecord?>()

    override suspend fun get(streamKey: String): RouteRecord? {
        val cached = cache.getIfPresent(streamKey)
        if (cached != null) return cached

        val record = delegate.get(streamKey)
        if (record != null) {
            cache.put(streamKey, record)
        }
        return record
    }

    override suspend fun list(): List<RouteRecord> = delegate.list()

    override suspend fun put(record: RouteRecord) {
        delegate.put(record)
        cache.put(record.streamKey, record)
    }

    override suspend fun delete(streamKey: String): Boolean {
        val deleted = delegate.delete(streamKey)
        cache.invalidate(streamKey)
        return deleted
    }

    override suspend fun isReady(): Boolean = delegate.isReady()

    override fun close() {
        cache.invalidateAll()
        delegate.close()
    }
}
