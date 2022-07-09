package net.wujunwei.hive.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;

/**
 * JDBC操作Hive
 * 注意: 需要先启动hiveserver2服务
 *
 * @author wujunwei
 * @email  1399952803@qq.com
 * @github https://github.com/wujunwei928
 * @blog   http://www.wujunwei.net
 */
public class HiveJdbcDemo {
    public static void main(String[] args) {
        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        //
        // 注意: pom.xml 中 hive-jdbc的版本要和数据平台中hive的版本一致, 否则会报 Required field ‘client_protocol‘ is unset!
        //
        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        //指定hiveserver2的url链接 jdbc:hive2://ip:port/database , database可省略, 默认 default
        String jdbcUrl = "jdbc:hive2://cdh01:10000";
        try {
            // 获取链接
            Connection conn = DriverManager.getConnection(jdbcUrl, "", "");
            // 获取Statement
            Statement stmt = conn.createStatement();

            // 指定查询SQL
            String sql = "select * from wujunwei.student";

            // 执行SQL
            ResultSet res = stmt.executeQuery(sql);

            // 循环读取结果
            while (res.next()) {
                System.out.println(res.getInt("id") + "\t" + res.getString("name"));
            }
        } catch (Exception e) {
            System.out.println("jdbc hive process fail");
            System.out.println(e.fillInStackTrace());
            return;
        }
    }
}
