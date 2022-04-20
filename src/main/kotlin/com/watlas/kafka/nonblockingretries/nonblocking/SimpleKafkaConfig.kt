package com.watlas.kafka.nonblockingretries.nonblocking

import com.fasterxml.jackson.databind.ser.std.StringSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.retrytopic.RetryTopicConfiguration
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder


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
    fun producerFactory(): ProducerFactory<Int?, String?>? {
        return DefaultKafkaProducerFactory(producerConfigs()!!)
    }

    @Bean
    fun producerConfigs(): Map<String, Any>? {
        val props: MutableMap<String, Any> = HashMap()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        // See https://kafka.apache.org/documentation/#producerconfigs for more properties
        return props
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<Int?, String?>? {
        return KafkaTemplate(producerFactory()!!)
    }

    }
