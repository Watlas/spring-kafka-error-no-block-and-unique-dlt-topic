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
        dltStrategy = DltStrategy.NO_DLT,
        exclude = [
            DeserializationException::class,
            SerializationException::class,
            MessageConversionException::class,
            ConversionException::class,
            MethodArgumentResolutionException::class,
            NoSuchMethodException::class,
            ClassCastException::class
        ]
    )

    @KafkaListener(
        id = "\${spring.kafka.consumer.group-id}",
        topics = ["\${topic}"],
    )
    fun onReceive(message: String, @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String) {
        try {
            log.info("processing message: $topic")
            throw Exception()
        }catch (e: Exception) {
           if(topic.contains("retry")){
             this.sendToKafka(message)
           }
            throw e;
            }

    }


    @Async
    fun sendToKafka(data: String?) {
        Thread.sleep(20000)
        val record: ProducerRecord<String, String> =  ProducerRecord<String, String>("my-global-topic-dlt", data)
        val future: ListenableFuture<SendResult<String, String>> = kafkaTemplate!!.send(record)
        future.addCallback(object : KafkaSendCallback<String, String> {
            override fun onSuccess(result: SendResult<String?, String?>?) {
                handleSuccess(data)
            }

            override fun onFailure(ex: KafkaProducerException) {
                handleFailure(data, record, ex)
            }
        })
    }

    private fun handleFailure(data: String?, record: ProducerRecord<String, String>, ex: KafkaProducerException) {
        println("failed to send message: $data")
    }

    private fun handleSuccess(data: String?) {
        println("successfully sent message: $data")
    }
}

