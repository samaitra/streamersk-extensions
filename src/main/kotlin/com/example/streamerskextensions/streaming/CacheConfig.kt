package com.example.streamerskextensions.streaming

import org.apache.ignite.cache.affinity.AffinityUuid
import org.apache.ignite.configuration.CacheConfiguration
import java.util.*
import java.util.concurrent.TimeUnit
import javax.cache.configuration.FactoryBuilder
import javax.cache.expiry.CreatedExpiryPolicy
import javax.cache.expiry.Duration

object CacheConfig {
    /**
     * Configure streaming cache.
     */
    fun wordCache(): CacheConfiguration<UUID, String> {
        val cfg = CacheConfiguration<UUID, String>("words")

        // Index all words streamed into cache.
        cfg.setIndexedTypes(UUID::class.java, String::class.java)

        // Sliding window of 1 seconds.
        cfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(CreatedExpiryPolicy(Duration(TimeUnit.SECONDS, 1))))
        return cfg
    }
}
