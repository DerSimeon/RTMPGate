package lol.simeon.rtmpgate.routes

import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CachedRouteStoreTest {
    @Test
    fun `cache updates on put and invalidates on delete`() = runBlocking {
        val backing = InMemoryRouteStore()
        val store = CachedRouteStore(backing, ttlSeconds = 60)
        val record = RouteRecord(
            streamKey = "test-key",
            target = "rtmp://localhost:1935/live/target",
            createdAtEpochMillis = 1,
            updatedAtEpochMillis = 2,
        )

        store.put(record)
        assertEquals(record, store.get("test-key"))

        store.delete("test-key")
        assertNull(store.get("test-key"))
    }
}
