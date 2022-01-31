package org.kishuomi.controller

import com.fasterxml.jackson.databind.ObjectMapper
import org.kishuomi.exceptions.*
import spark.Spark.exception

class ExceptionHandler(private val mapper: ObjectMapper) {

    init {
        exception(ConflictException::class.java) { e, _, res ->
            (res.apply {
                status(409)
                body(mapper.writeValueAsString(ErrorDto(e.message ?: "Conflict")))
            })
        }

        exception(KafkaInstanceAlreadyRegisteredException::class.java) { e, _, res ->
            (res.apply {
                status(409)
                body(mapper.writeValueAsString(ErrorDto(e.message ?: "Conflict")))
            })
        }

        exception(NotFoundException::class.java) { e, _, res ->
            (res.apply {
                status(404)
                body(mapper.writeValueAsString(ErrorDto(e.message ?: "Not found")))
            })
        }

        exception(InstanceNotFoundException::class.java) { e, _, res ->
            (res.apply {
                status(404)
                body(mapper.writeValueAsString(ErrorDto(e.message ?: "Not found")))
            })
        }

        exception(BadRequestException::class.java) { e, _, res ->
            (res.apply {
                status(400)
                body(mapper.writeValueAsString(ErrorDto(e.message ?: "Bad request")))
            })
        }

        exception(InvalidData::class.java) { e, _, res ->
            (res.apply {
                status(400)
                body(mapper.writeValueAsString(ErrorDto(e.message ?: "Bad request")))
            })
        }

        exception(Exception::class.java) { e, _, res ->
            (res.apply {
                status(500)
                body(mapper.writeValueAsString(ErrorDto(e.message ?: "Unknown error")))
            })
        }
    }
}