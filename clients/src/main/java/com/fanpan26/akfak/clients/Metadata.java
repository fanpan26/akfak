package com.fanpan26.akfak.clients;

import com.fanpan26.akfak.common.Cluster;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * @author fanyuepan
 */
public class Metadata {

    private Cluster cluster;
    private boolean needUpdate;
    private int version;
    /**
     * 上次刷新时间
     * */
    private long lastRefreshMs;
    /**
     * 上次成功刷新的时间
     * */
    private long lastSuccessfulRefreshMs;
    /**
     * 过期时间
     * */
    private final long metadataExpireMs;
    /**
     * 刷新重试时间
     * */
    private final long refreshBackoffMs;

    public Cluster fetch(){
        return this.cluster;
    }

    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.version = 0;
        this.cluster = Cluster.empty();
        this.needUpdate = false;
        //TODO
//        this.topics = new HashSet<String>();
//        this.listeners = new ArrayList<>();
//        this.needMetadataForAllTopics = false;
    }
    public synchronized void update(Cluster cluster, long now) {
    }

    public synchronized int requestUpdate() {
        this.needUpdate = true;
        return this.version;
    }

    /**
     * 查询距离下次更新的时间
     * */
    public synchronized long timeToNextUpdate(long nowMs) {
        //如果即将更新或者距离上次更新时间已经超过了过期时间，那么直接更新
        long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
        return Math.max(timeToExpire, timeToAllowUpdate);
    }

}
