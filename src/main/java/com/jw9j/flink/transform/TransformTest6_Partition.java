package com.jw9j.flink.transform;

import com.jw9j.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  重新分区
 *  union vs connect;
 * @Author jw9j
 * @create 2021/7/11 21:52
 */
public class TransformTest6_Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile("E:\\code\\FlinkTutorial\\src\\main\\resources\\sensorData.txt");


        DataStream<SensorReading> dataStream = inputStream.map(line-> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

//        inputStream.print("input");

        // 1. shuffle stream
        DataStream<String> shuffleStream  = inputStream.shuffle();
        shuffleStream.print("shuffle");

        // 2. keyBy 按hashCode 进行分区;
        dataStream.keyBy("id").print("keyBy");
        shuffleStream.print("shuffle");

        // 3. global ,将所有流合并到一个分区;
        dataStream.global().print("global");
        env.execute();

    }

    // 实现map
    public static class  MyMapper0 implements MapFunction<SensorReading,Tuple2<String, Integer>>{
        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), value.getId().length());
        }
    }


}
