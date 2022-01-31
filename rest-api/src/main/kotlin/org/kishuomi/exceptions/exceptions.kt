package org.kishuomi.exceptions

import java.lang.Exception

open class ApiException : Exception {
    constructor(message: String) : super(message)
}

class ConflictException : ApiException {
    constructor(message: String) : super(message)
}

class NotFoundException : ApiException {
    constructor(message: String) : super(message)
}

class BadRequestException : ApiException {
    constructor(message: String) : super(message)
}