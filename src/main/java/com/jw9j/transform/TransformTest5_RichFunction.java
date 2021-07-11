package com.jw9j.transform;

import com.jw9j.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Int;
import scala.Tuple3;

/**
 *  侧输出流 SideOutput, Connect ,comap;
 *  union vs connect;
 * @Author jw9j
 * @create 2021/7/11 21:52
 */
public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile("E:\\code\\FlinkTutorial\\src\\main\\resources\\sensorData.txt");

        // 转换成Java POJO
        DataStream<SensorReading> dataStream = inputStream.map(line-> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        DataStream<Tuple2<String,Integer>> resultStream = dataStream.map(new MyMapper());

        resultStream.print();


        env.execute();

    }

    // 实现map
    public static class  MyMapper0 implements MapFunction<SensorReading,Tuple2<String, Integer>>{
        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), value.getId().length());
        }
    }

    // 实现自定义richmapp function
    // Rich function 抽象类
    // 获取运行上下文;
    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        // 初始化工作,一般是定义状态或者建立数据库连接;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open");
        }

        // 关闭链接或者清理状态;
        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close");
        }

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(),getRuntimeContext().getIndexOfThisSubtask());
        }
    }

}
