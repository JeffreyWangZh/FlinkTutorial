package com.jw9j.windows;

import com.jw9j.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Int;

/**
 * 1. 窗口分配器 windows（）方法
 * 2. 窗口函数；
 *
 * 窗口函数分类： 增量聚合函数，全窗口函数；
 * @Author jw9j
 * @create 2021/7/13 1:00
 */
public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("E:\\code\\FlinkTutorial\\src\\main\\resources\\sensorData.txt");

        inputStream = env.socketTextStream("localhost",28080);
        // 转换成Java POJO
        DataStream<SensorReading> dataStream = inputStream.map(line-> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        // keyby  windows 滚动窗口 分配器
        SingleOutputStreamOperator<Integer> resultStream =  dataStream.keyBy("id")
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() { //

                    // 创建累加器
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        resultStream.print();

        // 滑动窗口
        dataStream.keyBy(SensorReading::getId)
                .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5)));

        // session 窗口
//        dataStream.keyBy(SensorReading::getId)
//                .window()

        env.execute();

    }
}
