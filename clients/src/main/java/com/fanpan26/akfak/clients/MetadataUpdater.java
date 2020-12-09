package com.fanpan26.akfak.clients;

import com.fanpan26.akfak.common.Node;
import com.fanpan26.akfak.common.protocol.types.Struct;

import java.util.List;

/**
 * @author fanyuepan
 */
interface MetadataUpdater {

    List<Node> fetchNodes();

    boolean isUpdateDue(long now);

    long maybeUpdate(long now);

    boolean maybeHandleDisconnection(ClientRequest request);

    boolean maybeHandleCompletedReceive(ClientRequest request,long now,Struct body);

    void requestUpdate();
}
