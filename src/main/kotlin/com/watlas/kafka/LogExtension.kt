package com.watlas.kafka

import org.slf4j.LoggerFactory

val Any.log get() = LoggerFactory.getLogger(this::class.java)