package com.example.streamerskextensions.kafka

import org.apache.ignite.Ignition
import org.apache.ignite.internal.util.typedef.internal.A
import org.apache.ignite.stream.kafka.KafkaStreamer
import org.apache.kafka.clients.consumer.ConsumerConfig

import org.apache.kafka.common.serialization.StringDeserializer
import java.util.*
import java.util.concurrent.TimeoutException
import kotlin.collections.HashMap

private const val TOPIC = "mytopic"
private const val DEFAULT_CACHE_NAME = "testCache"

fun main() {
    consumerStream(TOPIC)
}

/**
 * Consumes Kafka stream via Ignite.
 *
 * @param topic Topic name.
 * @param keyValMap Expected key value map.
 * @throws TimeoutException If timed out.
 * @throws InterruptedException If interrupted.
 */
@Throws(TimeoutException::class, InterruptedException::class)
private fun consumerStream(topic: String) {
    var kafkaStmr: KafkaStreamer<String?, String?>? = null
    val ignite = Ignition.start("src/main/resources/ignite-config.xml")

    // Get the cache.
    ignite.getOrCreateCache<String, String>(DEFAULT_CACHE_NAME)

    try {
        ignite.dataStreamer<String, String>(DEFAULT_CACHE_NAME).use { stmr ->
            stmr.allowOverwrite(true)
            stmr.autoFlushFrequency(1)

            // Configure Kafka streamer.
            kafkaStmr = KafkaStreamer()

            // Set Ignite instance.
            kafkaStmr?.setIgnite(ignite)

            // Set data streamer instance.
            kafkaStmr?.setStreamer(stmr)

            // Set the topic.
            kafkaStmr?.setTopic(Arrays.asList(topic))

            // Set the number of threads.
            kafkaStmr?.setThreads(1)

            // Set the consumer configuration.
            kafkaStmr?.setConsumerConfig(
                    createDefaultConsumerConfig("localhost:9092", "groupX"))
            kafkaStmr?.setMultipleTupleExtractor { record ->
                var entries: HashMap<String?, String?> = HashMap()
                try {
                    val key = UUID.randomUUID().toString()
                    val value = record.value() as String
                    entries[key] = value
                } catch (ex: Exception) {
                    ex.printStackTrace()
                }
                entries
            }

            // Start kafka streamer.
            kafkaStmr?.start()

        }
    } catch (e: Exception) {
        e.printStackTrace()
    }
}

/**
 * Creates default consumer config.
 *
 * @param servers Bootstrap servers' address in the form of &lt;server:port;server:port&gt;.
 * @param grpId Group Id for kafka subscriber.
 * @return Kafka consumer configuration.
 */
private fun createDefaultConsumerConfig(servers: String, grpId: String): Properties {
    A.notNull(servers, "bootstrap servers")
    A.notNull(grpId, "groupId")
    val props = Properties()
    props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = servers
    props[ConsumerConfig.GROUP_ID_CONFIG] = grpId
    props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
    props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
    return props
}


