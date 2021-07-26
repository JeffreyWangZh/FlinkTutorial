package com.jw9j.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Flink sink kafka:
 * 1. 引用 Pom
 *  <dependency>
 *             <groupId>org.apache.flink</groupId>
 *             <artifactId>flink-connector-kafka_2.11</artifactId>
 *             <version>1.13.0</version>
 *         </dependency>
 *
 *2. new FlinkKafkaProducer();
 *
 * 3. 在kafka 中查看  kafka-console-consumer --bootstrap-server localhost:9092 --topic sinkTest
 * @Author jw9j
 * @create 2021/7/12 1:17
 */
public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

//        DataStream<String> inputStream = env.readTextFile("E:\\code\\FlinkTutorial\\src\\main\\resources\\sensorData.txt");
//
//        // 转换成Java POJO
//        DataStream<String> dataStream = inputStream.map(line-> {
//            String[] fields = line.split(",");
//            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2])).toString();
//        });

        // 从kafka 读取 并在写入
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:29092");
        properties.setProperty("group.id","consumer-group");
        properties.setProperty("auto.offset.reset","latest");

        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("sensor",new SimpleStringSchema(),properties));

        // 2. 将数据写入到kafka;
        dataStream.addSink(new FlinkKafkaProducer<String>("localhost:29092","sinkTest",new SimpleStringSchema()));

        env.execute();

    }
}
