package com.machenxu.es.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

public class ElasticSearchClientTest {

    private TransportClient client;

    @Before
    public void init() throws Exception {
        //创建一个Settings对象
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();
        //创建一个TransPortClient对象
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.124.65"), 9300));

    }

    @Test
    public void createIndex() throws Exception {
        //创建一个Settings对象
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();
        //创建一个TransPortClient对象
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.124.65"), 9300));
        //3、使用client对象创建一个索引库
        client.admin().indices().prepareCreate("eslog4")
                //执行操作
                .get();
        //4、关闭client对象
        client.close();
    }

    @Test
    public void setMappings() throws Exception {
        //创建一个Settings对象
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();
        //创建一个TransPortClient对象
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.124.65"), 9300));
        //创建一个Mappings信息
        /*{
            "article":{
            "properties":{
                "id":{
                    "type":"long",
                            "store":true
                },
                "title":{
                    "type":"text",
                            "store":true,
                            "index":true,
                            "analyzer":"ik_smart"
                },
                "content":{
                    "type":"text",
                            "store":true,
                            "index":true,
                            "analyzer":"ik_smart"
                }
            }
        }
        }*/
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("OdinLog")
                .startObject("properties")
                .startObject("id")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("biz")
                .field("type", "string")
                .endObject()
                .startObject("business")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("category")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("param")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("level")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("content")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("createTime")
                .field("type", "date")
                .field("format", "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
/*
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("OdinLog")
                        .startObject("properties")
                            .startObject("id")
                                .field("type","text")
                                .field("store", true)
                            .endObject()
                            .startObject("biz")
                                .field("type", "text")
                                .field("store", true)
                            .endObject()
                            .startObject("business")
                                .field("type", "text")
                                .field("store", true)
                            .endObject()
                            .startObject("category")
                                .field("type", "text")
                                .field("store", true)
                            .endObject()
                            .startObject("param")
                                .field("type", "text")
                                .field("store", true)
                            .endObject()
                            .startObject("level")
                                .field("type", "text")
                                .field("store", true)
                            .endObject()
                            .startObject("levelStr")
                                .field("type", "text")
                                .field("store", true)
                            .endObject()
                            .startObject("content")
                                .field("type", "text")
                                .field("store", true)
                            .endObject()
                            .startObject("createTime")
                                .field("type", "text")
                                .field("store", true)
                            .endObject()
                            .startObject("startTime")
                                .field("type", "text")
                                .field("store", true)
                            .endObject()
                            .startObject("endTime")
                                .field("type", "text")
                                .field("store", true)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
*/
        //使用client把mapping信息设置到索引库中
        client.admin().indices()
                //设置要做映射的索引
                .preparePutMapping("eslog4")
                //设置要做映射的type
                .setType("OdinLog")
                //mapping信息，可以是XContentBuilder对象可以是json格式的字符串
                .setSource(builder)
                //执行操作
                .get();
        //关闭链接
        client.close();
    }

    @Test
    public void testAddDocument() throws Exception {
        //创建一个client对象
        //创建一个文档对象
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("title","北方入秋速度明显加快 多地降温幅度最多可达10度22222")
                .field("content", "阿联酋一架客机在纽约机场被隔离 10名乘客病倒")
                .endObject();
        //把文档对象添加到索引库
        client.prepareIndex()
                //设置索引名称
                .setIndex("index_hello")
                //设置type
                .setType("article")
                //设置文档的id，如果不设置的话自动的生成一个id
                //.setId("2")
                //设置文档信息
                .setSource(builder)
                //执行操作
                .get();
        //关闭客户端
        client.close();
    }

    @Test
    public void testAddDocument2() throws Exception {
        //创建一个Article对象
      /*  Article article = new Article();
        //设置对象的属性
        article.setId(3l);
        article.setTitle("MH370坠毁在柬埔寨密林?中国一公司调十颗卫星去拍摄");
        article.setContent("警惕荒唐的死亡游戏!俄15岁少年输掉游戏后用电锯自杀");*/
        Log l =new Log();
        l.setBiz("测试log");
        l.setBusiness("测试json");
        //把article对象转换成json格式的字符串。
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonDocument = objectMapper.writeValueAsString(l);
        System.out.println(jsonDocument);
        //使用client对象把文档写入索引库
        client.prepareIndex("eslog","log", "3")
                .setSource(jsonDocument, XContentType.JSON)
                .get();
        //关闭客户端
        client.close();
    }

    @Test
    public void testAddDocument3() throws Exception {
        for (int i = 4; i < 100; i++) {
            //创建一个Article对象
            Article article = new Article();
            //设置对象的属性
            article.setId(i);
            article.setTitle("女护士路遇昏迷男子跪地抢救：救人是职责更是本能" + i);
            article.setContent("江西变质营养餐事件已致24人就医 多名官员被调查" + i);
            //把article对象转换成json格式的字符串。
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonDocument = objectMapper.writeValueAsString(article);
            System.out.println(jsonDocument);
            //使用client对象把文档写入索引库
            client.prepareIndex("index_hello","article", i + "")
                    .setSource(jsonDocument, XContentType.JSON)
                    .get();

        }
        //关闭客户端
        client.close();
    }
}
