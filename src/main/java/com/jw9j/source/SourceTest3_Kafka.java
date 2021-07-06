package com.jw9j.source;

<<<<<<< HEAD
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 从kafka读取数据
 *
 * 1. 引用pom
 * 2. 开启kafka 创建生产者,
 *      kafka-console-producer -bootstrap-server localhost:9092 --topic sensor
 * @Author jw9j
 * @create 2021/7/7 1:20
 */
public class SourceTest3_Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:29092");
        properties.setProperty("group.id","consumer-group");
        properties.setProperty("auto.offset.reset","latest");

        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("sensor",new SimpleStringSchema(),properties));
//        DataStream<String> dataStream = env.addSource()

        dataStream.print();

        env.execute();
    }
}
