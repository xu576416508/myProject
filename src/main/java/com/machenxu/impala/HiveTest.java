package com.machenxu.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class HiveTest {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {

        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
        	Class.forName(driverName);
        	conn = DriverManager.getConnection("jdbc:hive2://192.168.1.66:21050/default;auth=noSasl", "hdfs", "hdfs");
        	//preparedStatement = conn.prepareStatement("select name,substr(name,3) from testcreate");
            //preparedStatement = conn.prepareStatement("alter table testcreate add COLUMNS (time1 timestamp)");
        	//preparedStatement = conn.prepareStatement("delete from testcreate where id=2");
        	//preparedStatement = conn.prepareStatement("insert into testCreate (id,name,time1) values (7,'timetest','2019-05-12 16:28:56')");//插入记录时，时间类型直接按string处理即可
        	//preparedStatement = conn.prepareStatement("select id,decode(id,1,'a',2,'b','c') as id_a from testcreate");
        	//preparedStatement = conn.prepareStatement("select time1,to_timestamp('2019-05-12 23:12:68','yyyy-mm-dd HH:mm:ss') as time3,from_unixtime(unix_timestamp(time1),'yyyy-MM-dd HH:mm:ss') as time2 from testcreate");
            //查询所有表名 show tables in default
            //查询某表的列信息 show column stats testCreate
            //查询某表的列信息 DESCRIBE test1
        	//preparedStatement = conn.prepareStatement("create table geotest ( id int ,name geography)");
            //preparedStatement.executeUpdate();
        	
        	
        	//preparedStatement = conn.prepareStatement("select time1,id,decode(id,1,'a',2,'b','c') as id_a from testcreate where time1 between '2018-12-12 12:12:12' and '2019-05-13 23:23:23'");
        	preparedStatement = conn.prepareStatement("select name,substr(name,2,3) as name1 from testcreate where name is not null");
        	
        	//preparedStatement = conn.prepareStatement("select name,instr(name,'t',1,2) as name1 from testcreate where name is not null");
        	//preparedStatement = conn.prepareStatement("select id,name,time1,substr(cast(time1 as string),1,19) as time2,cast('2019-12-21 23:53:33' as timestamp) as mytime1,to_timestamp('2019-05-12 18:19:58','yyyy-mm-dd HH:mm:ss') as mytime from testcreate where time1 is not null limit 5");
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData md = resultSet.getMetaData();//获取键名
            int columnCount = md.getColumnCount();//获取行的数量
            int j = 0;
            while (resultSet.next()){
            	System.out.println(">>>>>>>>>>>>>>>>>>>>>");
            	for (int i = 1; i <= columnCount; i++) {
            		System.out.println("==="+md.getColumnName(i)+"<<<<>>>>"+resultSet.getObject(i));//获取键名及值
        		}
            	j++;
            }
            System.out.println(j);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }finally {
            try {
            	conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        
    }


}
