package com.jw9j.flink.window;

import com.jw9j.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** 计数窗口的测试
 * @author Zenghui Wang
 * @create 2021-07-14 10:11 PM
 */
public class WindowsTest2_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStream<String> inputStream = env.readTextFile("D:\\JustProject\\00-jw9j\\FlinkTurtorial\\src\\main\\resources\\sensorData.txt");

        DataStream<String> inputStream = env.socketTextStream("localhost",9200);
        // 转换成Java POJO
        DataStream<SensorReading> dataStream = inputStream.map(line-> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        // 开计数窗口测试
        DataStream<Double> avgTemResultStream =  dataStream.keyBy(SensorReading::getId)
                .countWindow(10,2)
                .aggregate(new MyAvgTemp());

        avgTemResultStream.print();
        env.execute();
    }
    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple2<Double,Integer>,Double>{

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading sensorReading, Tuple2<Double, Integer> doubleIntegerTuple2) {
            return new Tuple2<>(doubleIntegerTuple2.f0+sensorReading.getTemperature(),doubleIntegerTuple2.f1+1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> doubleIntegerTuple2) {
            return doubleIntegerTuple2.f0/doubleIntegerTuple2.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> doubleIntegerTuple2, Tuple2<Double, Integer> acc1) {
            return null;
        }
    }

}
