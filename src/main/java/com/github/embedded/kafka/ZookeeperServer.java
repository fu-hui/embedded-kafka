package com.github.embedded.kafka;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class ZookeeperServer {
    public void asyncInitAndStart() {
        Executor executor = Executors.newSingleThreadExecutor();
        executor.execute(() -> {
            final String path = ZookeeperServer.class.getClassLoader().getResource("config/zookeeper.properties").getPath();
            QuorumPeerMain.main(new String[]{path});
        });
    }
}
