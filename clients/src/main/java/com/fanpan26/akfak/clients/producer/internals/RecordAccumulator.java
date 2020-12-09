package com.fanpan26.akfak.clients.producer.internals;

import com.fanpan26.akfak.common.Cluster;
import com.fanpan26.akfak.common.Node;

import java.util.HashSet;
import java.util.Set;

/**
 * @author fanyuepan
 */
public final class RecordAccumulator {


    /**
     * 查找准备好的节点
     * */
    public ReadyCheckResult ready(Cluster cluster, long nowMs){
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long .MAX_VALUE;
        boolean unknownLeadersExist = false;

        //TODO  Buffer Pool
        //boolean exhausted = this.free.queued() > 0;
        //返回检查结果 已经准备发送数据的Node，下一次检查是否Ready的时间间隔，是否leader不存在
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeadersExist);
    }

    public final static class ReadyCheckResult {

        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final boolean unknownLeadersExist;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, boolean unknownLeadersExist) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeadersExist = unknownLeadersExist;
        }
    }
}
