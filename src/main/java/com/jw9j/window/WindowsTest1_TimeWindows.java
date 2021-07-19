package com.jw9j.window;

import com.jw9j.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * 窗口类型：
 * 1. 时间窗口
 *  滚动时间窗口 tumbling
 *  .timeWindow(Time)
 *  滑动时间窗口 slide
 *  会话窗口 session
 *
 * 其他API
 *
 * @author Zenghui Wang
 * @create 2021-07-13 8:59 PM
 */
public class WindowsTest1_TimeWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStream<String> inputStream = env.readTextFile("D:\\JustProject\\00-jw9j\\FlinkTurtorial\\src\\main\\resources\\sensorData.txt");

        DataStream<String> inputStream = env.socketTextStream("localhost",20098);
        // 转换成Java POJO
        DataStream<SensorReading> dataStream = inputStream.map(line-> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        // 进行windows 操作必须使用keyBY
        // windows Assigner 窗口分配器
        // 1. 增量聚合
//        DataStream<Integer> resultStream = dataStream.keyBy(SensorReading::getId)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                //
//                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
//                    @Override
//                    public Integer createAccumulator() {
//                        return  0;
//                    }
//
//                    @Override
//                    public Integer add(SensorReading sensorReading, Integer integer) {
//                        return integer+1;
//                    }
//
//                    @Override
//                    public Integer getResult(Integer integer) {
//                        return integer;
//                    }
//
//                    // session window 合并操作；
//                    @Override
//                    public Integer merge(Integer integer, Integer acc1) {
//                        return integer + acc1;
//                    }
//                });

        // 2. 全窗口函数
        SingleOutputStreamOperator<Tuple3<String,Long,Integer>> resultStream2 =  dataStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = s;
                        Long windowEnd = window.getEnd();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id,windowEnd,count));
                    }
                });

        // 其他可选Api；
        // allow
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late"){

        };
        SingleOutputStreamOperator<SensorReading> sumStream =  dataStream.keyBy(SensorReading::getId)
//                .timeWindow(Time.seconds(12))
//                .allowedLateness()
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .sum("temperature");

        sumStream.getSideOutput(outputTag).print("late");
        resultStream2.print();
        env.execute();
    }
}
