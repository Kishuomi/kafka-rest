package org.kishuomi.common

import org.apache.kafka.clients.admin.AdminClient

fun AdminClient.getTopicListNames(includeInternal: Boolean = false): List<String> =
    listTopics().namesToListings().get().values
        .filter { !it.isInternal || (includeInternal && it.isInternal) }
        .map { it.name() }
        .toList()