package com.jw9j.flink.sink;

import com.jw9j.flink.beans.SensorReading;
import com.jw9j.flink.source.SourceTest4_UDF;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * JDBC 自定义sink
 *
 * 1. 添加MySQL 连接器
 *
 *
 * @author Zenghui Wang
 * @create 2021-07-12 9:20 PM
 */
public class SinkTest4_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("D:\\JustProject\\00-jw9j\\FlinkTurtorial\\src\\main\\resources\\sensorData.txt");

        // 转换成Java POJO
        DataStream<SensorReading> dataStream = inputStream.map(line-> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        // 模拟实时生产数据
        dataStream = env.addSource(new SourceTest4_UDF.MySensorSource());

        dataStream.addSink(new MyJDBCSink());
        env.execute();
    }

    public static class MyJDBCSink extends RichSinkFunction<SensorReading> {


        Connection connection = null;
        PreparedStatement inserStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            // 每来一条语句，调用连接，执行sql；
            updateStmt.setDouble(1,value.getTemperature());
            updateStmt.setString(2,value.getId());
            updateStmt.execute();
            if(updateStmt.getUpdateCount()==0){
                inserStmt.setDouble(2,value.getTemperature());
                inserStmt.setString(1,value.getId());
                inserStmt.execute();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://10.126.21.99:3306/bluemo","root","aA2N!eVy93Vg");
            inserStmt = connection.prepareStatement("insert into sensor_temp(id,temp) value(?,?)");
            updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        @Override
        public void close() throws Exception {
            inserStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}
