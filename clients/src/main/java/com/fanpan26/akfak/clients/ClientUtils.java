package com.fanpan26.akfak.clients;

import com.fanpan26.akfak.common.network.ChannelBuilder;
import com.fanpan26.akfak.common.network.ChannelBuilders;
import com.fanpan26.akfak.common.network.LoginType;
import com.fanpan26.akfak.common.network.Mode;
import com.fanpan26.akfak.common.network.SecurityProtocol;

import java.util.Map;

/**
 * @author fanyuepan
 */
public class ClientUtils {

    public static ChannelBuilder createChannelBuilder(Map<String, ?> configs) {
        SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        return ChannelBuilders.create(securityProtocol, Mode.CLIENT, LoginType.CLIENT, configs, "", true);
    }
}
