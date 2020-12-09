package com.fanpan26.akfak.server;

import com.fanpan26.akfak.network.SocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author fanyuepan
 */
public class KafkaServer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaServer.class);

    private KafkaConfig config;

    private BrokerState brokerState = BrokerState.NOT_RUNNING;

    private SocketServer socketServer;

    private AtomicBoolean startupComplete = new AtomicBoolean(false);
    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private AtomicBoolean isStartingUp = new AtomicBoolean(false);

    private CountDownLatch shutdownLatch = new CountDownLatch(1);

    public KafkaServer(KafkaConfig config) {
        this.config = config;
        socketServer = new SocketServer(config);
    }

    public void start() {
        if (isShuttingDown.get()) {
            throw new IllegalStateException("Kafka Server is still shutting down,cannot re-start");
        }
        if (startupComplete.get()) {
            logger.info("Kafka Server is already started.");
            return;
        }
        boolean canStart = isStartingUp.compareAndSet(false, true);
        if (canStart) {
            brokerState = BrokerState.STARTING;
            //TODO startup zookeeper
            //TODO startup logManger
            config.setBrokerId(newBrokerId());
            socketServer.start();

            //TODO other things
            brokerState = BrokerState.RUNNING_AS_BROKER;
            shutdownLatch = new CountDownLatch(1);
            startupComplete.set(true);
            isStartingUp.set(false);
        }
    }

    private int newBrokerId() {
        //TODO use zookeeper generate a brokerId
        return 0;
    }

    public void shutdown() throws Exception{

    }

    public void awaitShutdown() {
        try {
            shutdownLatch.await();
        }catch (InterruptedException e){

        }
    }
}
