package com.fanpan26.akfak.clients;

import com.fanpan26.akfak.common.Cluster;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * @author fanyuepan
 */
public class Metadata {

    private Cluster cluster;

    public Cluster fetch(){
        return this.cluster;
    }

    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        this.cluster = Cluster.empty();
    }

    public synchronized void update(Cluster cluster, long now) {
    }
}
