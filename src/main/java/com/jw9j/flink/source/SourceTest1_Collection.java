package com.jw9j.flink.source;

import com.jw9j.flink.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 *  从集合中读取数据源
 * @Author jw9j
 * @create 2021/7/7 0:58
 */
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取source
        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor1",15453121L,15.4),
                new SensorReading("sensor2",15453122L,15.5),
                new SensorReading("sensor3",15453123L,15.6)));

        DataStream<Integer> integerDataStream = env.fromElements(1,2,3,4,5);

        // 3. 转化操作
        dataStream.print("dataStream");
        integerDataStream.print("IntegerStream");

        // 4. 执行
        env.execute();
    }
}
