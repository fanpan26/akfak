package com.fanpan26.akfak.network;

import com.fanpan26.akfak.cluster.EndPoint;
import com.fanpan26.akfak.common.network.SecurityProtocol;
import com.fanpan26.akfak.common.network.Selectable;
import com.fanpan26.akfak.common.utils.Utils;
import com.fanpan26.akfak.server.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author fanyuepan
 */
public class SocketServer {

    private static final Logger logger  = LoggerFactory.getLogger(SocketServer.class);

    private KafkaConfig config;

    private Map<SecurityProtocol,EndPoint> listeners;
    private Map<EndPoint,Acceptor> acceptors = new HashMap<>();
    private List<Processor> processors = new ArrayList<>();

    private int numProcessorThreads;

    private int maxQueuedRequests;

    private int totalProcessorThreads;

    private int maxConnectionPerIp;

    private RequestChannel requestChannel;

    private Selectable selector;

    public SocketServer(KafkaConfig config) {
        this.config = config;
    }

    public RequestChannel getRequestChannel() {
        return requestChannel;
    }

    public void start() {
        logger.info("SocketServer starting...");

        listeners = config.getListeners();
        numProcessorThreads = config.getNumNetworkThreads();
        maxQueuedRequests = config.getQueuedMaxRequests();
        totalProcessorThreads = numProcessorThreads * listeners.size();
        maxConnectionPerIp = config.getMaxConnectionPerIp();
        requestChannel = new RequestChannel(numProcessorThreads, maxQueuedRequests);

        int sendBufferSize = config.getSendBufferSize();
        int receiveBufferSize = config.getReceiveBufferSize();

        int brokerId = config.getBrokerId();
        int processorBeginIndex = 0;
        for (Map.Entry<SecurityProtocol, EndPoint> entry : listeners.entrySet()) {
            SecurityProtocol protocolType = entry.getValue().getProtocol();
            int processorEndIndex = processorBeginIndex + numProcessorThreads;
            //初始化processor
            for (int i = processorBeginIndex; i < processorEndIndex; i++) {
                logger.info("Initializing {} processor", i);
                processors.add(i, newProcessor(i, requestChannel, protocolType));
            }
            //初始化 acceptor
            Acceptor acceptor = new Acceptor(entry.getValue(),
                    sendBufferSize,
                    receiveBufferSize,
                    brokerId,
                    processors.subList(processorBeginIndex, processorEndIndex));
            acceptors.put(entry.getValue(), acceptor);

            String threadName = String.format("kafka-socket-acceptor-%s-%d", protocolType.toString(), entry.getValue().getPort());
            logger.info("Start acceptor thread:{}", threadName);
            Utils.newThread(threadName, acceptor, false).start();
            acceptor.awaitStartup();
            processorBeginIndex = processorEndIndex;
        }
    }

    private Processor newProcessor(int id,RequestChannel requestChannel,SecurityProtocol protocol){
        return new Processor(id,
                config.getSocketRequestMaxSize(),
                requestChannel,
                config.getConnectionsMaxIdleMs(),
                protocol,config.values());
    }

}
