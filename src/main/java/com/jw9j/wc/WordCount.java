package com.jw9j.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

// 批处理word count
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2, 从文件中读取数据
        String inputPath = "src/main/resources/wordcount.txt";
        // DataSet
        DataSource<String>  inputDataSet = env.readTextFile(inputPath);

        //3，对数据集进行处理 flatmap

        // 也可以使用 DataSource 多态的特点

        DataSet<Tuple2<String,Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                // 按照第一个位置及字符串进行分词
                .groupBy(0)
                // 二元组第二个位置
                .sum(1);

        // 4. 打印输出；
        resultSet.print();
    }

    // 自定义类实现flatmap function
    // 2元组 Tuple2
    //? int vs Interger
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            // 按空格分期
            String[] words = s.split(" ");

            // 遍历所有word,包成二元组
            for(String word : words)
            {
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
