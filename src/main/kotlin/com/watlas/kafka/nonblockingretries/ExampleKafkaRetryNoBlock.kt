package com.watlas.kafka.nonblockingretries

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ExampleKafkaRetryNoBlockApplication

fun main(args: Array<String>) {
    runApplication<ExampleKafkaRetryNoBlockApplication>(*args)
}
