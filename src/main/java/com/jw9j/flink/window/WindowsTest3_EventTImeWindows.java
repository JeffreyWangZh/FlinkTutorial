package com.jw9j.flink.window;

import com.jw9j.flink.beans.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 事件语义
 * 定义事件语义，在stream中使用 TimeCharacteristic
 * @author Zenghui Wang
 * @create 2021-07-19 5:59 PM
 */
public class WindowsTest3_EventTImeWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost",7777);

        //转换成sensorreading类型
        DataStream<SensorReading> dataStream = inputStream.map(line->{
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new WatermarkStrategy<SensorReading>() {
            @Override
            public WatermarkGenerator<SensorReading> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return null;
            }
        });//

        env.execute();
    }
}
