package com.examples.danielpetisme;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
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
public class SampleWithStateStore {

    private static final String KAFKA_ENV_PREFIX = "KAFKA_";
    public static final String STORE_NAME = "store";
    private final Logger logger = LoggerFactory.getLogger(SampleWithStateStore.class);
    private final KafkaStreams streams;
    private final Topics topics;

    public SampleWithStateStore(Map<String, String> config) throws InterruptedException, ExecutionException {
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

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_NAME),
                        Serdes.String(),
                        Serdes.String()
                ));

        var inputA = builder.stream(
                topics.aTopic,
                Consumed.with(Serdes.String(), Serdes.String()));
        var inputB = builder.stream(
                topics.bTopic,
                Consumed.with(Serdes.String(), Serdes.String()));

        inputA.transform(StreamAggregator::new, STORE_NAME).to(topics.outputTopic);
        inputB.transform(StreamAggregator::new, STORE_NAME).to(topics.outputTopic);

        return builder.build();

    }

    private static class StreamAggregator implements Transformer<String, String, KeyValue<String, String>> {

        private KeyValueStore<String, String> store;

        @Override
        public void init(ProcessorContext context) {
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {
            var old = store.putIfAbsent(key, value);
            if (old != null) {
                store.put(key, String.join(",", old, value));
            }
            return KeyValue.pair(key, store.get(key));
        }

        @Override
        public void close() {

        }
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
            StreamsConfig.APPLICATION_ID_CONFIG, System.getenv().getOrDefault("APPLICATION_ID", SampleWithStateStore.class.getName()),
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
