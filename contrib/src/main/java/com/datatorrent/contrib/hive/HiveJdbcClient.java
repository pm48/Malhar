/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.hive;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HiveJdbcClient
{
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";

  public static void main(String[] args) throws SQLException
  {

    try {
      Class.forName(driverName);
    }
    catch (ClassNotFoundException ex) {
      Logger.getLogger(HiveJdbcClient.class.getName()).log(Level.SEVERE, null, ex);
    }

    Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
    Statement stmt = con.createStatement();
    String tableName = "temp4";
    stmt.execute("drop table temp4");
    stmt.execute("CREATE TABLE IF NOT EXISTS temp4 (col1 map<string,int>,col2 map<string,int>,col3 map<string,int>,col4 map<String,int>,col5 map<string,int>) \n"
            + "row format SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'  \n"
           // + "WITH SERDEPROPERTIES (“input.regex” = “([^ ]*) ([^ ]*) ([^ ]*) (-|\\\\[[^\\\\]]*\\\\]) ([^ \\\"]*|\\”[^\\\"]*\\”) ”),“output.format.string”=”%1$s %2$s %3$s %4$s %5$s”)  \n"
            + "COLLECTION ITEMS TERMINATED BY ','  \n"
            + "MAP KEYS TERMINATED BY ':'  \n"
            + "LINES TERMINATED BY '\n'  \n"
            + "STORED AS TEXTFILE ");
    String filepath = "/user/README.txt";

    String sql = "load data inpath '" + filepath + "' into table " + tableName;
    stmt.execute(sql);


    // show tables
    sql = "show tables '" + tableName + "'";
    System.out.println("Running: " + sql);
    ResultSet res = stmt.executeQuery(sql);
    if (res.next()) {
      System.out.println(res.getString(1));
    }


    // select * query
    sql = "select * from temp4";
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
     while (res.next()) {
      System.out.println(res.getString(1));
    }

    // regular hive query
    sql = "select count(*) from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1));

    }
  }

}
