package com.jw9j.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author jw9j
 * @create 2021/7/7 1:13
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 按原有顺序输出；
        //env.setParallelism(1);

        String filePath = "E:\\code\\FlinkTutorial\\src\\main\\resources\\sensorData.txt";
        DataStream<String>  dataStream= env.readTextFile(filePath);

        dataStream.print();
        env.execute();

    }
}
