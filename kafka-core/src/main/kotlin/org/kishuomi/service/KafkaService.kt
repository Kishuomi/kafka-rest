package org.kishuomi.service

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.kishuomi.common.getTopicListNames
import org.kishuomi.exceptions.InstanceNotFoundException
import org.kishuomi.exceptions.KafkaInstanceAlreadyRegisteredException
import org.kishuomi.models.*
import org.slf4j.LoggerFactory
import java.util.*

class KafkaService {

    private val logger = LoggerFactory.getLogger(KafkaService::class.java)

    private val kafkaInstances = mutableMapOf<String, KafkaInstance>()

    fun connectToInstance(instanceCreate: KafkaInstanceCreate): KafkaInstanceDescription {
        instanceCreate.validate()

        kafkaInstances[instanceCreate.name]?.apply { throw KafkaInstanceAlreadyRegisteredException("Instance with name=${instanceCreate.name} already exists!") }

        val kafkaInstance = kafkaInstances.computeIfAbsent(instanceCreate.name) {
            val adminClient = AdminClient.create(
                Properties().apply {
                    put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, instanceCreate.url)
                    put(AdminClientConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString())
                    put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000)
                }
            )

            val kafkaProducer = Properties().apply {
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, instanceCreate.url)
            }.let {
                KafkaProducer(it, StringSerializer(), StringSerializer())
            }

            KafkaInstance(
                instanceCreate.name,
                instanceCreate.description,
                instanceCreate.url,
                adminClient,
                kafkaProducer
            )
        }

        return kafkaInstance.toInstanceDescription()
    }

    fun getState(): List<KafkaInstanceDescription> = kafkaInstances.values
        .map { it.toInstanceDescription() }
        .toList()

    fun get(kafkaInstanceName: String) = getInstance(kafkaInstanceName).toInstanceDescription()

    fun getTopicsDescriptions(kafkaInstanceName: String): List<TopicDescription> {
        val adminClient = getAdminClient(kafkaInstanceName)
        val topicListNames = adminClient.getTopicListNames()
        val describeTopics = adminClient.describeTopics(topicListNames)
        return describeTopics.all().get().values.map {
            TopicDescription(
                it.name(),
                it.partitions().size,
                it.partitions()[0].replicas().size
            )
        }.toList()
    }

    fun createTopic(kafkaInstanceName: String, topicCreate: TopicCreate): TopicDescription {
        topicCreate.validate()
        val adminClient = getAdminClient(kafkaInstanceName)
        val newTopicsList =
            listOf(NewTopic(topicCreate.name, topicCreate.partitionsNumber, topicCreate.replicationFactor))
        val createTopics = adminClient.createTopics(newTopicsList)

        return TopicDescription(
            name = topicCreate.name,
            partitionsNumber = createTopics.numPartitions(topicCreate.name).get(),
            replicationFactor = createTopics.replicationFactor(topicCreate.name).get()
        )
    }

    fun getTopicDetails(kafkaInstanceName: String, topicName: String): TopicDetails {
        val adminClient = getAdminClient(kafkaInstanceName)
        val topicDescription = adminClient.describeTopics(listOf(topicName)).all().get()[topicName]!!
        val partitionsDetails = topicDescription.partitions()
            .map { TopicPartition(topicName, it.partition()) }
            .map {
                val earliest = adminClient.listOffsets(mapOf(Pair(it, OffsetSpec.earliest()))).partitionResult(it).get()
                val latest = adminClient.listOffsets(mapOf(Pair(it, OffsetSpec.latest()))).partitionResult(it).get()
                PartitionDetails(
                    partition = it.partition(),
                    earliestOffset = earliest.offset(),
                    earliestOffsetTimestamp = earliest.timestamp(),
                    latestOffset = latest.offset(),
                    latestOffsetTimestamp = latest.timestamp()
                )
            }
            .toList()
        return TopicDetails(
            name = topicName,
            partitionsNumber = topicDescription.partitions().size,
            replicationFactor = topicDescription.partitions().first().replicas().size,
            partitionsDetails = partitionsDetails
        )
    }

    fun getConsumerGroupsDescriptions(kafkaInstanceName: String): List<ConsumerGroupDescription> {
        val adminClient = getAdminClient(kafkaInstanceName)
        val groupIds = adminClient.listConsumerGroups().all().get().map { it.groupId() }.toList()
        return adminClient.describeConsumerGroups(groupIds).all().get().values
            .map { ConsumerGroupDescription(it.groupId(), it.state().name) }
    }

    fun getConsumerGroupDetails(kafkaInstanceName: String, consumerGroupId: String): ConsumerGroupDetails {
        val adminClient = getAdminClient(kafkaInstanceName)
        val groupDescription = adminClient.describeConsumerGroups(listOf(consumerGroupId)).all().get().values.first()
        val topicPartitionsMap =
            adminClient.listConsumerGroupOffsets(consumerGroupId).partitionsToOffsetAndMetadata().get()
        val topicInfosMap = mutableMapOf<String, MutableList<PartitionForConsumerGroup>>()

        for (entry in topicPartitionsMap) {
            topicInfosMap.putIfAbsent(entry.key.topic(), mutableListOf())
            val latest =
                adminClient.listOffsets(mapOf(Pair(entry.key, OffsetSpec.latest()))).partitionResult(entry.key).get()
            val lag = latest.offset() - entry.value.offset()
            topicInfosMap[entry.key.topic()]!!.add(
                PartitionForConsumerGroup(
                    partition = entry.key.partition(),
                    lag = lag,
                    groupOffset = entry.value.offset(),
                    latestOffset = latest.offset(),
                    latestOffsetTimestamp = latest.timestamp()
                )
            )
        }

        return ConsumerGroupDetails(
            groupId = groupDescription.groupId(),
            state = groupDescription.state().name,
            topicInfos = topicInfosMap.map { TopicForConsumerGroup(it.key, it.value) }.toList()
        )
    }

    fun send(kafkaInstanceName: String, message: ProducerMessage) {
        getProducer(kafkaInstanceName).apply {
            this.send(message.create()).get()
        }
    }

    fun stop() {
        kafkaInstances.values.forEach { it.adminClient.close() }
        logger.info("KafkaService stopped")
    }

    private fun getInstance(kafkaInstanceName: String): KafkaInstance {
        val found = kafkaInstances[kafkaInstanceName]
        if (found != null) {
            return found
        } else {
            throw InstanceNotFoundException("Instance with name=${kafkaInstanceName} not found!")
        }
    }

    private fun getAdminClient(kafkaInstanceName: String) = getInstance(kafkaInstanceName).adminClient

    private fun getProducer(kafkaInstanceName: String) = getInstance(kafkaInstanceName).producer

    private fun ProducerMessage.create(): ProducerRecord<String, String> {
        return ProducerRecord(topic, partition, key, value)
    }
}