package com.github.embedded.kafka;

import kafka.server.KafkaServerStartable;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class KafkaServer {
    private KafkaServerStartable kafkaServerStartable;

    public void asyncInitAndStart() throws IOException {
        final String path = KafkaServer.class.getClassLoader().getResource("config/kafka-server.properties").getPath();
        final Properties properties;
        properties = Utils.loadProps(path);
        asyncInitAndStart(properties);
    }

    public void asyncInitAndStart(Properties properties) {
        Executor executor = Executors.newSingleThreadExecutor();
        executor.execute(() -> {
            kafkaServerStartable = KafkaServerStartable.fromProps(properties);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaServerStartable.shutdown()));
            kafkaServerStartable.startup();
            kafkaServerStartable.awaitShutdown();
        });
    }
}
