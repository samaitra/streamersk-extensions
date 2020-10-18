package com.example.streamerskextensions.streaming

import com.example.streamerskextensions.utils.ExamplesUtils
import org.apache.ignite.IgniteCache
import org.apache.ignite.Ignition
import java.io.File
import java.util.*

object StreamWords {
    /**
     * Starts words streaming.
     *
     * @param args Command line arguments (none required).
     * @throws Exception If failed.
     */
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        // Mark this cluster member as client.
        Ignition.setClientMode(true)
        Ignition.start("examples/config/example-ignite.xml").use { ignite ->
            if (!ExamplesUtils.hasServerNodes(ignite)) return

            // The cache is configured with sliding window holding 1 second of the streaming data.
            val stmCache: IgniteCache<UUID, String> = ignite.getOrCreateCache(CacheConfig.wordCache())
            ignite.dataStreamer<UUID, String>(stmCache.name).use { stmr ->
                while (true) {
                        File("examples/alice.txt").forEachLine { line->
                            while (line != null) {
                                for (word in line.split(" ").toTypedArray()){
                                    if (word.isNotEmpty()) { // Stream words into Ignite.
                                    val key = UUID.randomUUID()
                                    stmr.addData(key, word)
                                    }
                                }

                            }

                        }
                }
            }
        }
    }
}