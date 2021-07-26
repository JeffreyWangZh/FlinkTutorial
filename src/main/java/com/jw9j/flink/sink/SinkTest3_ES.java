package com.jw9j.flink.sink;

import com.jw9j.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * write the data to es
 * 1. 引用Pom
 *<dependency>
 *             <groupId>org.apache.flink</groupId>
 *             <artifactId>flink-connector-elasticsearch7_2.12</artifactId>
 *             <version>1.13.1</version>
 *         </dependency>
 * @author Zenghui Wang
 * @create 2021-07-12 6:57 PM
 */
public class SinkTest3_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("D:\\JustProject\\00-jw9j\\FlinkTurtorial\\src\\main\\resources\\sensorData.txt");

        // 转换成Java POJO
        DataStream<SensorReading> dataStream = inputStream.map(line-> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        // 定义es的连接配置
        ArrayList<HttpHost> httpPosts = new ArrayList<>();
        httpPosts.add(new HttpHost("localhost",9200));


        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpPosts,new MyEsSinkFunction()).build());

        env.execute();

    }
    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading>{
        @Override
        public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            // 定义写入的数据source；
            HashMap<String,String> dataSource = new HashMap<>();
            dataSource.put("id",sensorReading.getId());
            dataSource.put("temp",sensorReading.getTemperature().toString());
            dataSource.put("ts",sensorReading.getTimeStamp().toString());
            // 创建请求 作为项es发起请求
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("readingdata")
                    .source(dataSource);

            // 发送index 请求
            requestIndexer.add(indexRequest);
        }
    }
}
