package org.kishuomi

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.kishuomi.controller.ExceptionHandler
import org.kishuomi.controller.KafkaController
import org.kishuomi.service.KafkaService
import spark.Spark.port
import spark.kotlin.after
import spark.servlet.SparkApplication

class Application(port: Int) : SparkApplication {

    private val kafkaService = KafkaService()
    private val mapper = ObjectMapper().registerKotlinModule()

    init {
        port(port)
        after {
            response.header("Content-Type", "application/json")
        }
        KafkaController(mapper, kafkaService)
        ExceptionHandler(mapper)
    }

    override fun init() {
        //nothing
    }

    override fun destroy() {
        kafkaService.stop()
        super.destroy()
    }
}