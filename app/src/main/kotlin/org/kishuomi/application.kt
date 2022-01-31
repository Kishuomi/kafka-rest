package org.kishuomi


fun main(args: Array<String>) {
    val application = Application(8081)
    Runtime.getRuntime().addShutdownHook(Thread { application.destroy() })
}