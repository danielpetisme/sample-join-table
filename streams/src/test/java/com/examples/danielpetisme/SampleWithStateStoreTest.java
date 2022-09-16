package com.examples.danielpetisme;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class SampleWithStateStoreTest {

    KafkaProducer<String, String> inputProducer;
    KafkaConsumer<String, String> aConsumer;
    KafkaConsumer<String, String> bConsumer;
    KafkaConsumer<String, String> outputConsumer;
    Topics topics;
    SampleWithStateStore streams;

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1"));

    @Before
    public void beforeEach() throws ExecutionException, InterruptedException {
        topics = new Topics(AdminClient.create(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
        )));

        inputProducer = new KafkaProducer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE
                ),
                new StringSerializer(), new StringSerializer()
        );

        aConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "aConsumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(), new StringDeserializer()
        );
        aConsumer.subscribe(Collections.singletonList(topics.aTopic));

        bConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "bConsumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(), new StringDeserializer()
        );
        bConsumer.subscribe(Collections.singletonList(topics.bTopic));

        outputConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "outputConsumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(), new StringDeserializer()
        );

        outputConsumer.subscribe(Collections.singletonList(topics.outputTopic));
    }

    @After
    public void afterEach() {
        streams.stop();
    }


    @Test
    public void test() throws Exception {

        streams = new SampleWithStateStore(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        StreamsConfig.APPLICATION_ID_CONFIG, this.getClass().getName() + "-" + UUID.randomUUID(),
                        StreamsConfig.STATE_DIR_CONFIG, Paths.get("target", this.getClass().getName(), UUID.randomUUID().toString()).toString()
                )
        );
        streams.start();

        try {
            inputProducer.send(
                    new ProducerRecord<>(topics.aTopic, "1", "a"),
                    (RecordMetadata metadata, Exception exception) -> {
                        if (exception != null) {
                            fail(exception.getMessage());
                        }
                        System.out.println("Produced " + metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
                    }
            ).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        try {
            inputProducer.send(
                    new ProducerRecord<>(topics.bTopic, "1", "b"),
                    (RecordMetadata metadata, Exception exception) -> {
                        if (exception != null) {
                            fail(exception.getMessage());
                        }
                        System.out.println("Produced " + metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
                    }
            ).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        List<KeyValue<String, String>> loaded = new ArrayList<>();

        long start = System.currentTimeMillis();
        while (loaded.isEmpty() && System.currentTimeMillis() - start < 20_000) {
            ConsumerRecords<String, String> records = outputConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            records.forEach((record) -> loaded.add(new KeyValue<>(record.key(), record.value())));
        }
        assertThat(loaded).isNotEmpty();
        assertThat(loaded).contains(KeyValue.pair("1", "a"), KeyValue.pair("1", "a,b"));

        loaded.forEach(it -> {
            System.out.println(it.key + ", " + it.value);
        });

    }

}