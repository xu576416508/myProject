package com.machenxu.es.test;

import com.machenxu.es.util.ESUtil;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

public class ESTest {
    
    @Test
    public void add() throws Exception {
        TransportClient client = ESUtil.getClient();
        //创建一个client对象
        //创建一个文档对象
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id",2l)
                .field("title","南方入秋速度明显加快 多地降温幅度最多可达10度22222")
                .field("content", "阿联酋一架客机在纽约机场被隔离 10名乘客病倒")
                .endObject();
        //把文档对象添加到索引库
        client.prepareIndex()
                //设置索引名称
                .setIndex("index_hello")
                //设置type
                .setType("article")
                //设置文档的id，如果不设置的话自动的生成一个id
              //  .setId("2")
                //设置文档信息
                .setSource(builder)
                //执行操作
                .get();
        //关闭客户端
        client.close();

    }
    
}
