package com.fanpan26.akfak.server;

/**
 * @author fanyuepan
 */
public class KafkaServerStarter {

    private KafkaServer server;

    public KafkaServerStarter(KafkaConfig config){
        server = new KafkaServer(config);
    }

    public void shutdown(){
        try {
            server.shutdown();
        }catch (Exception e){
            Runtime.getRuntime().halt(1);
        }
    }

    public void start(){
        server.start();
    }

    public void awaitShutdown(){
        server.awaitShutdown();
    }
}
