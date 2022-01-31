package org.kishuomi.models

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.Producer
import org.kishuomi.exceptions.InvalidData

data class KafkaInstanceCreate(
    val name: String,
    val description: String?,
    val url: String
)

fun KafkaInstanceCreate.validate() {
    if (this.name.contains(" ")) throw InvalidData("KafkaInstance's name must not contains any spaces!")
}

data class KafkaInstanceDescription(
    val name: String,
    val description: String?,
    val url: String,
)

data class KafkaInstance(
    val name: String,
    val description: String?,
    val url: String,
    val adminClient: AdminClient,
    val producer: Producer<String, String>
)

fun KafkaInstance.close() {
    this.adminClient.close()
    this.producer.close()
}

data class TopicCreate(
    val name: String,
    val partitionsNumber: Int = 1,
    val replicationFactor: Short = 1
)

fun TopicCreate.validate() {
    if (this.name.contains(" ")) throw InvalidData("Topic's name must not contains any spaces!")
}

data class TopicDescription(
    val name: String,
    val partitionsNumber: Int,
    val replicationFactor: Int
)

data class TopicDetails(
    val name: String,
    val partitionsNumber: Int,
    val replicationFactor: Int,
    val partitionsDetails: List<PartitionDetails>
)

data class PartitionDetails(
    val partition: Int,
    val earliestOffset: Long,
    val earliestOffsetTimestamp: Long,
    val latestOffset: Long,
    val latestOffsetTimestamp: Long
)

data class ConsumerGroupDescription(
    val groupId: String,
    val state: String
)

data class ConsumerGroupDetails(
    val groupId: String,
    val state: String,
    val topicInfos: List<TopicForConsumerGroup>
)

data class TopicForConsumerGroup(
    val topicName: String,
    val partitionInfo: List<PartitionForConsumerGroup>
)

data class PartitionForConsumerGroup(
    val partition: Int,
    val lag: Long,
    val groupOffset: Long,
    val latestOffset: Long,
    val latestOffsetTimestamp: Long
)

fun KafkaInstance.toInstanceDescription() = KafkaInstanceDescription(name, description, url)

data class ProducerMessage(
    val topic: String,
    val partition: Int?,
    val key: String?,
    val value: String?
)