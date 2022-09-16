package com.examples.danielpetisme;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class Topics {

    private final Logger logger = LoggerFactory.getLogger(Topics.class);

    public final String aTopic;
    public final String bTopic;
    public final String tableTopic;
    public final String outputTopic;

    public Topics(AdminClient adminClient) throws ExecutionException, InterruptedException {
        aTopic = System.getenv().getOrDefault("A_TOPIC", "a");
        bTopic = System.getenv().getOrDefault("B_TOPIC", "b");
        tableTopic = System.getenv().getOrDefault("TABLE_TOPIC", "table");
        outputTopic = System.getenv().getOrDefault("OUTPUT_EVENT_TOPIC", "output");

        final Integer numberOfPartitions = Integer.valueOf(System.getenv().getOrDefault("NUMBER_OF_PARTITIONS", "1"));
        final Short replicationFactor = Short.valueOf(System.getenv().getOrDefault("REPLICATION_FACTOR", "1"));


        createTopic(adminClient, aTopic, numberOfPartitions, replicationFactor);
        createTopic(adminClient, bTopic, numberOfPartitions, replicationFactor);
        createTopic(adminClient, tableTopic, numberOfPartitions, replicationFactor);
        createTopic(adminClient, outputTopic, numberOfPartitions, replicationFactor);
    }

    public void createTopic(AdminClient adminClient, String topicName, Integer numberOfPartitions, Short replicationFactor) throws InterruptedException, ExecutionException {
        if (!adminClient.listTopics().names().get().contains(topicName)) {
            logger.info("Creating topic {}", topicName);
            final NewTopic newTopic = new NewTopic(topicName, numberOfPartitions, replicationFactor);
            try {
                CreateTopicsResult topicsCreationResult = adminClient.createTopics(Collections.singleton(newTopic));
                topicsCreationResult.all().get();
            } catch (ExecutionException e) {
                //silent ignore if topic already exists
            }
        }
    }
}
