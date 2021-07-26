package com.jw9j.flink.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {

        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 数据读取

        // 从文件读取数据
        // String inputPath = "E:\\code\\FlinkTutorial\\src\\main\\resources\\wordcount.txt";
        // DataStream<String> inputStream = env.readTextFile(inputPath);
        // 使用Parameter tool工具从程序启动参数提取配置项

        // 从端口接受数据
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStream<String> inputStream = env.socketTextStream(host,port);
        // 核心 DataStream


        // 3. 数据处理
        DataStream<Tuple2<String,Integer>> resultStrean =  inputStream.flatMap(new WordCount.MyFlatMapper())
                // 与 批处理区别， group by vs keyBy
                // 按照当前key hash 进行充分去
                .keyBy(0)
                .sum(1);

        // 4. 打印数据
        resultStrean.print();

        // 5. 事件触发，启动以上步骤。
        env.execute();

    }
}
