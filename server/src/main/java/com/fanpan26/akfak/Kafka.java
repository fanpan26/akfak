package com.fanpan26.akfak;

import com.fanpan26.akfak.server.KafkaConfig;
import com.fanpan26.akfak.server.KafkaServerStarter;

/**
 * 服务端主启动类
 * @author fanyuepan
 */
public class Kafka {

    public static void main(String[] args) {

        try {
            KafkaConfig config = KafkaConfig.defaultConfig();
            KafkaServerStarter starter = new KafkaServerStarter(config);
            starter.start();
            starter.awaitShutdown();
        } catch (Exception e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
