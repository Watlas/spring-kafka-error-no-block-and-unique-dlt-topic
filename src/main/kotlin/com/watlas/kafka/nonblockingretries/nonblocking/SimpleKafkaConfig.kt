package com.watlas.kafka.nonblockingretries.nonblocking

import com.fasterxml.jackson.databind.ser.std.StringSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.retrytopic.*
import org.springframework.kafka.retrytopic.RetryTopicNamesProviderFactory.RetryTopicNamesProvider
import java.time.Clock


@EnableKafka
@Configuration
class SimpleKafkaConfig {

    @Bean
    fun consumerFactory(properties: KafkaProperties): ConsumerFactory<String?, String?> =
        DefaultKafkaConsumerFactory(properties.buildConsumerProperties())


    @Bean
    fun kafkaListenerContainerFactory(properties: KafkaProperties): ConcurrentKafkaListenerContainerFactory<String?, String?> =
        ConcurrentKafkaListenerContainerFactory<String?, String?>()
            .apply { consumerFactory = consumerFactory(properties) }

    @Bean
    fun kafkaErrorProducerFactory(kafkaProperties: KafkaProperties): DefaultKafkaProducerFactory<String, String> =
        DefaultKafkaProducerFactory(kafkaProperties.buildProducerProperties())

    @Bean
    fun kafkaTemplate(kafkaProperties: KafkaProperties): KafkaTemplate<String, String> =
        KafkaTemplate(kafkaErrorProducerFactory(kafkaProperties))

    @Bean
    fun kafkaTemplate(): KafkaTemplate<Int?, String?>? {
        return KafkaTemplate(producerFactory()!!)
    }

    @Bean
    fun providerFactory(): RetryTopicNamesProviderFactory? {
        return object : SuffixingRetryTopicNamesProviderFactory() {
            override fun createRetryTopicNamesProvider(properties: DestinationTopic.Properties): RetryTopicNamesProvider {
                return if (properties.isDltTopic) {
                    object : SuffixingRetryTopicNamesProvider(properties) {
                        override fun getTopicName(topic: String): String {
                            return "my.dlt"
                        }
                    }
                } else super.createRetryTopicNamesProvider(properties)
            }
        }
    }

    @Bean(name = [RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME])
    fun topicResolver(
        applicationContext: ApplicationContext?,

        ): DefaultDestinationTopicResolver? {
        return DefaultDestinationTopicResolver(Clock.systemUTC(), applicationContext)
    }

}
