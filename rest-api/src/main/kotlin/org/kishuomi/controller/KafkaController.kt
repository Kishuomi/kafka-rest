package org.kishuomi.controller

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.kishuomi.models.KafkaInstanceCreate
import org.kishuomi.models.ProducerMessage
import org.kishuomi.models.TopicCreate
import org.kishuomi.service.KafkaService
import org.slf4j.LoggerFactory
import spark.Spark.path
import spark.kotlin.*

class KafkaController(
    private val mapper: ObjectMapper,
    private val kafkaService: KafkaService
) {

    private val logger = LoggerFactory.getLogger(KafkaController::class.java)

    init {
        //log incoming request
        before("/*") {
            val reqHeaders = request.headers().associateWith { request.headers(it) }
            logger.debug("Request, body=${request.body()}, params=${request.params()}, headers=$reqHeaders, url=${request.url()}")
        }
        //log outgoing response
        after("/*") {
            logger.debug("Response, body=${response.body()}")
        }
        path("/kafka") {
            //connect to Kafka instance
            post("") {
                request.body()
                    .let { mapper.readValue<KafkaInstanceCreate>(it) }
                    .let { kafkaService.connectToInstance(it) }
                    .let { mapper.writeValueAsString(it) }
                    .apply { response.body(this) }
            }
            //Get info about all Kafka connections
            get("") {
                kafkaService.getState().let { mapper.writeValueAsString(it) }
            }
            //Get info about Kafka connection by name
            get("/:name") {
                kafkaService.get(request.params(":name")).let { mapper.writeValueAsString(it) }
            }
            //Disconnect from instance
            delete("/:name") {
                kafkaService.disconnectFromInstance(request.params(":name"))
            }
            //Send message
            post("/:name/send") {
                request.body()
                    .let { mapper.readValue<ProducerMessage>(it) }
                    .apply {
                        kafkaService.send(request.params(":name"), this)
                        response.body("Success")
                        response.status(200)
                    }
            }
            //Topic section
            path("/:name/topic") {
                //Get list of topics for specified Kafka instance
                get("") {
                    kafkaService.getTopicsDescriptions(request.params(":name")).let { mapper.writeValueAsString(it) }
                }
                //Create topic
                post("") {
                    request.body()
                        .let { mapper.readValue<TopicCreate>(it) }
                        .let { kafkaService.createTopic(request.params(":name"), it) }
                        .let { mapper.writeValueAsString(it) }
                        .apply { response.body(this) }
                }
                //Get Topic's details
                get("/:topicName/details") {
                    kafkaService.getTopicDetails(request.params(":name"), request.params(":topicName"))
                        .let { mapper.writeValueAsString(it) }
                }
            }
            //Consumer group section
            path("/:name/consumergroups") {
                get("") {
                    kafkaService.getConsumerGroupsDescriptions(request.params(":name"))
                        .let { mapper.writeValueAsString(it) }
                }
                get("/:consumerGroupId/details") {
                    kafkaService.getConsumerGroupDetails(request.params(":name"), request.params(":consumerGroupId"))
                        .let { mapper.writeValueAsString(it) }
                }
            }
        }
    }
}