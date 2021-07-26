package com.jw9j.flink.transform;

import com.jw9j.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;
import scala.Tuple3;

/**
 *  侧输出流 SideOutput, Connect ,comap;
 *  union vs connect;
 * @Author jw9j
 * @create 2021/7/11 21:52
 */
public class TransformTest4_SideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("E:\\code\\FlinkTutorial\\src\\main\\resources\\sensorData.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line-> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        // 定义测输出流的名字
        final OutputTag<String> outputTag1 = new OutputTag<String>("low"){};
        final OutputTag<String> outputTag2 = new OutputTag<String>("high"){};


        // 分流
        SingleOutputStreamOperator<SensorReading> resultDataStream = dataStream.process(new CustomSideOutput(outputTag1,outputTag2));

        DataStream<String> sideOutputDataStream = resultDataStream.getSideOutput(outputTag1);
        DataStream<String> sideOutputhighDataStream = resultDataStream.getSideOutput(outputTag2);



        sideOutputDataStream.print("low -> ");
        sideOutputhighDataStream.print("high->");
//        resultDataStream.print("all ->");

        // 2. 合流Connect, 将高温流转换成二元组类型,与低温流链接合并之后,输出状态信息;
        DataStream<Tuple2<String,Double>> warningStream  = sideOutputhighDataStream.map(new MapFunction<String, Tuple2<String,Double>>() {
            @Override
            public Tuple2<String, Double> map(String value) throws Exception {
                String[] fields = value.replace(")","").split(",");
                return new Tuple2<String,Double>(fields[0].split("=")[1],new Double(fields[2].split("=")[1]));
            }
        });

        // 连接
        ConnectedStreams<Tuple2<String, Double>, String> connectedStreams = warningStream.connect(sideOutputDataStream);
        
        DataStream<Object> connectedStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, String, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {

                return new Tuple3<>(value._1,value._2,"hign temp warning");
            }

            @Override
            public Object map2(String value) throws Exception {
                String[] field = value.split(",");
                return new Tuple3<>(field[0].split("=")[1],field[1].split("=")[1],"low temp");
            }
        });

        connectedStream.print();

        // 3. union 连接多条流;
        sideOutputhighDataStream.union(sideOutputhighDataStream).print("union");

        env.execute();


    }

    // 侧输出流;
    public static class CustomSideOutput extends ProcessFunction<SensorReading,SensorReading>{
        private OutputTag<String> outputTag1;
        private OutputTag<String> outputTag2;

        public CustomSideOutput(OutputTag<String> outputTag1, OutputTag<String> outputTag2) {
            this.outputTag1 = outputTag1;
            this.outputTag2 = outputTag2;
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            if(value.getTemperature() < 60) {
                String msg = value.getId() + "的温度低于 60° -> " + value.getTemperature();
                ctx.output(outputTag1, value.toString());
            }
            else if(value.getTemperature()>60){
                ctx.output(outputTag2,value.toString());
            }
                out.collect(value);
        }
    }
}
