package lol.simeon.rtmpgate.routes

import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class InMemoryRouteStoreTest {
    @Test
    fun `stores lists and deletes routes`() = runBlocking {
        val store = InMemoryRouteStore()
        val record = RouteRecord(
            streamKey = "test-key",
            target = "rtmp://localhost:1935/live/target",
            createdAtEpochMillis = 1,
            updatedAtEpochMillis = 2,
        )

        store.put(record)

        assertEquals(record, store.get("test-key"))
        assertEquals(listOf(record), store.list())
        assertTrue(store.delete("test-key"))
        assertFalse(store.delete("test-key"))
        assertNull(store.get("test-key"))
    }
}
