package com.jw9j.flink.sink;

import com.jw9j.flink.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * sink to redis
 * 1. add pom
 *
 * @author Zenghui Wang
 * @create 2021-07-12 6:21 PM
 */
public class SinkTest2_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("D:\\JustProject\\00-jw9j\\FlinkTurtorial\\src\\main\\resources\\sensorData.txt");

        // 转换成Java POJO
        DataStream<SensorReading> dataStream = inputStream.map(line-> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        // 定义jedis连接
        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(26379)
                .build();

        dataStream.addSink(new RedisSink<>(flinkJedisPoolConfig,new MyRedisMapper()));

        env.execute();


    }
    public static class MyRedisMapper implements RedisMapper<SensorReading>{

        // 定义保存数据到redis的命令, 存成hash表，hset， 表名 sensor_temp
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor_temp");
        }


        //
        @Override
        public String getKeyFromData(SensorReading data) {
            return data.getId();
        }

        @Override
        public String getValueFromData(SensorReading data) {
            return data.getTemperature().toString();
        }
    }
}
