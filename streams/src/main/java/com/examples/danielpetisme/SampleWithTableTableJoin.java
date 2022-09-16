package com.examples.danielpetisme;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class SampleWithTableTableJoin {

    private static final String KAFKA_ENV_PREFIX = "KAFKA_";
    public static final String STORE_NAME = "store";
    private final Logger logger = LoggerFactory.getLogger(SampleWithTableTableJoin.class);
    private final KafkaStreams streams;
    private final Topics topics;

    public SampleWithTableTableJoin(Map<String, String> config) throws InterruptedException, ExecutionException {
        var properties = buildProperties(defaultProps, System.getenv(), KAFKA_ENV_PREFIX, config);
        AdminClient adminClient = KafkaAdminClient.create(properties);
        this.topics = new Topics(adminClient);

        var topology = buildTopology();
        logger.info(topology.describe().toString());
        logger.info("creating streams with props: {}", properties);
        streams = new KafkaStreams(topology, properties);
        streams.setUncaughtExceptionHandler((exception -> {
            exception.printStackTrace();
            logger.info("An error occurred {}. Shutting down the app", exception.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        }));
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    Topology buildTopology() {

        var builder = new StreamsBuilder();

        var inputA = builder.table(
                topics.aTopic,
                Consumed.with(Serdes.String(), Serdes.String()));
        var inputB = builder.table(
                topics.bTopic,
                Consumed.with(Serdes.String(), Serdes.String()));

        inputA.outerJoin(inputB, (left, right) -> String.join(",", left, right)).toStream().to(topics.outputTopic);


        return builder.build();

    }

    public void start() {
        logger.info("Kafka Streams started");
        streams.start();
    }

    public void stop() {
        streams.close(Duration.ofSeconds(3));
        logger.info("Kafka Streams stopped");
    }

    private Map<String, String> defaultProps = Map.of(
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            StreamsConfig.APPLICATION_ID_CONFIG, System.getenv().getOrDefault("APPLICATION_ID", SampleWithTableTableJoin.class.getName()),
            StreamsConfig.REPLICATION_FACTOR_CONFIG, "1",
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName(),
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName(),
            StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest"
    );

    private Properties buildProperties(Map<String, String> baseProps, Map<String, String> envProps, String prefix, Map<String, String> customProps) {
        Map<String, String> systemProperties = envProps.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(
                        e -> e.getKey()
                                .replace(prefix, "")
                                .toLowerCase()
                                .replace("_", ".")
                        , e -> e.getValue())
                );

        Properties props = new Properties();
        props.putAll(baseProps);
        props.putAll(systemProperties);
        props.putAll(customProps);
        return props;
    }
}
