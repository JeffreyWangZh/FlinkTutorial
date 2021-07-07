package com.jw9j.transform;

import com.jw9j.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.crypto.Data;

/**
 * 基于keyby的滚动聚合操作
 * @Author jw9j
 * @create 2021/7/8 0:07
 */
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("E:\\code\\FlinkTutorial\\src\\main\\resources\\sensorData.txt");

        // 转换成SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                String[] fields = s.split(",");
//                return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
//            }
//        });
        //  转换成SensorReading类型 lambda

        DataStream<SensorReading> dataStream = inputStream.map(line-> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        // 分组；
        KeyedStream<SensorReading, Tuple>  keyedStream =  dataStream.keyBy("id");

        // 分组 方法2
//        KeyedStream<SensorReading,String> keyedStream1 = dataStream.keyBy(data-> data.getId());

        // 滚动聚合，获取当前传感器最大的值；
        // max 和 maxBy的区别
//        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.maxBy("temperature");
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.max("temperature");

        resultStream.print();

        env.execute();

    }
}
