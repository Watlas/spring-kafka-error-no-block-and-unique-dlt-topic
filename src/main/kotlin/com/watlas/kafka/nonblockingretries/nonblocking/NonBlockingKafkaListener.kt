package com.watlas.kafka.nonblockingretries.nonblocking

import com.watlas.kafka.log
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.SerializationException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.core.KafkaProducerException
import org.springframework.kafka.core.KafkaSendCallback
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.retrytopic.DltStrategy
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.SendResult
import org.springframework.kafka.support.converter.ConversionException
import org.springframework.kafka.support.serializer.DeserializationException
import org.springframework.messaging.converter.MessageConversionException
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException
import org.springframework.messaging.support.GenericMessage
import org.springframework.retry.annotation.Backoff
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component
import org.springframework.util.concurrent.ListenableFuture


@Component
class NonBlockingKafkaListener{

    @Autowired
    private val kafkaTemplate: KafkaTemplate<String, String>? = null


    @RetryableTopic(
        attempts = "\${retry-attempts}",
        backoff = Backoff(delay = 200, multiplier = 3.0, maxDelay = 0),
        numPartitions = "1",
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        dltStrategy = DltStrategy.FAIL_ON_ERROR,
    )

    @KafkaListener(
        id = "\${spring.kafka.consumer.group-id}",
        topics = ["\${topic}"],
    )
    fun onReceive(message: String, @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String) {
            log.info("processing message: $topic")
            throw Exception()

    }


    @RetryableTopic(
        attempts = "\${retry-attempts}",
        backoff = Backoff(delay = 200, multiplier = 3.0, maxDelay = 0),
        numPartitions = "1",
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        dltStrategy = DltStrategy.FAIL_ON_ERROR,
    )

    @KafkaListener(
//        id = "\${spring.kafka.consumer.group-id}",
        topics = ["\${topic2}"],
    )
    fun onReceive2(message: String, @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String) {
        log.info("processing message: $topic")
        throw Exception()

    }

}

