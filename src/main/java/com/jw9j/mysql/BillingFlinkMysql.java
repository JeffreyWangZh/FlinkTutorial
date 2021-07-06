package com.jw9j.mysql;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  读取账单数据
 * @Author jw9j
 * @create 2021/7/6 22:36
 */
public class BillingFlinkMysql {
    private static SourceFromMysql sourceFromMysql;
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SourceFromMysql()).print();
        env.execute();
    }
}
