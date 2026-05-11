package lol.simeon.rtmpgate.routes

import java.util.concurrent.ConcurrentHashMap

class InMemoryRouteStore : RouteStore {
    private val routes = ConcurrentHashMap<String, RouteRecord>()

    override suspend fun get(streamKey: String): RouteRecord? = routes[streamKey]

    override suspend fun list(): List<RouteRecord> = routes.values.sortedBy { it.streamKey }

    override suspend fun put(record: RouteRecord) {
        routes[record.streamKey] = record
    }

    override suspend fun delete(streamKey: String): Boolean = routes.remove(streamKey) != null
}
