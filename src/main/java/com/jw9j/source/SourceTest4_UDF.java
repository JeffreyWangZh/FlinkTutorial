package com.jw9j.source;

import com.jw9j.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * 自定义数据源
 * @Author jw9j
 * @create 2021/7/7 23:30
 */
public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        dataStream.print();

        env.execute();
    }

    // 实现自定义方法；
    // 模拟温度传感器的生成数据
    public static class MySensorSource implements SourceFunction<SensorReading>{

        // 生成数据控制 方法 标志位
        private boolean running = true;
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            // 定义随机数发生器
            Random random = new Random();
            // 设置10个传感器的初始温度；
            HashMap<String,Double> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                // 生成高斯分布的随机数；
                sensorTempMap.put("sensor_"+(i+1),30 + random.nextGaussian()*10);
            }
            while(running){
                for (String sensorId:sensorTempMap.keySet()) {
                    // 在当前温度基础上随机波动
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId,newTemp);
                    ctx.collect(new SensorReading(sensorId,System.currentTimeMillis(),newTemp));
                }
                // k控制成成频率
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
