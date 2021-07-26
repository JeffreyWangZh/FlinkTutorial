package com.jw9j.flink.transform;

import com.jw9j.flink.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  reduce
 *
 * @Author jw9j
 * @create 2021/7/8 1:14
 */
public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("E:\\code\\FlinkTutorial\\src\\main\\resources\\sensorData.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line-> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });
        // 分组；
        KeyedStream<SensorReading, Tuple> keyedStream =  dataStream.keyBy("id");

        // reduce聚合，取最大温度值，并所对应的最新时间；
//        keyedStream.reduce(new ReduceFunction<SensorReading>() {
//            @Override
//            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
//                return new SensorReading(value1.getId(), value2.getTimeStamp(),Math.max(value1.getTemperature(), value2.getTemperature()));
//            }
//        });

        DataStream<SensorReading> resultStream = keyedStream.reduce((curData,newData)->{
            return new SensorReading(curData.getId(), newData.getTimeStamp(), Math.max(curData.getTemperature(), newData.getTemperature()));
        });

        resultStream.print();

        env.execute();

    }
}
