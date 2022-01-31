package org.kishuomi.exceptions

data class ErrorDto(
    val message: String,
    val cause: ErrorDto? = null
)