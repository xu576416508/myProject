package com.machenxu.es.util;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.net.InetAddress;


public  class  ESUtil {
    public static TransportClient getClient() throws Exception{
        //创建一个Settings对象
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();
        //创建一个TransPortClient对象
        TransportClient   client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.0.105"), 9300));
        
        return client;
    }
}
