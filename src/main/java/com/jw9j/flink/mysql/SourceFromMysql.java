package com.jw9j.flink.mysql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author jw9j
 * @create 2021/7/6 23:13
 */
public class SourceFromMysql extends RichSourceFunction<AzureBillingDetail> {
    PreparedStatement ps;
    private Connection connection;

    /**
     * 每当调用函数时，首先创建数据库链接，并在结束后取消
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "select * from azureoriginalbillingdetail;";
        ps = this.connection.prepareStatement(sql);
    }

    /**
     * 调用结束，释放资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if(connection!=null){
            connection.close();
        }
        if(ps!=null){
            ps.close();
        }
    }

    /**
     * dataStream 调用run方法获取数据
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<AzureBillingDetail> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while(resultSet.next()){
            AzureBillingDetail azureBillingDetail = new AzureBillingDetail(
                    resultSet.getNString(0),
                    resultSet.getNString(1),
                    resultSet.getNString(2),
                    resultSet.getNString(3),
                    resultSet.getNString(4),
                    resultSet.getNString(5),
                    resultSet.getNString(6),
                    resultSet.getNString(7),
                    resultSet.getNString(8),
                    resultSet.getNString(9),
                    resultSet.getNString(10),
                    resultSet.getNString(11),
                    resultSet.getNString(12),
                    resultSet.getNString(13),
                    resultSet.getNString(14),
                    resultSet.getNString(15),
                    resultSet.getNString(16),
                    resultSet.getNString(17),
                    resultSet.getNString(18),
                    resultSet.getNString(19),
                    resultSet.getNString(20),
                    resultSet.getNString(21),
                    resultSet.getNString(22),
                    resultSet.getNString(23),
                    resultSet.getNString(24),
                    resultSet.getNString(25),
                    resultSet.getNString(26),
                    resultSet.getNString(27),
                    resultSet.getNString(26),
                    resultSet.getNString(29),
                    resultSet.getNString(30),
                    resultSet.getNString(31),
                    resultSet.getNString(32),
                    resultSet.getNString(33),
                    resultSet.getNString(34)
            );
            sourceContext.collect(azureBillingDetail);
        }
    }

    @Override
    public void cancel() {

    }

    private static Connection getConnection(){
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://10.126.21.99:3306/bluemo?serverTimezone=GMT%2B8&characterEncoding=UTF-8&autoReconnect=true&rewriteBatchedStatements=true", "root", "aA2N!eVy93Vg");
        }catch (Exception e){

            System.out.println("**********"+e.getMessage());
        }
        return con;
    }
}
