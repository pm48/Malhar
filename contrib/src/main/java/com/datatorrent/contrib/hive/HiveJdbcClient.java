/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.hive;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcClient
{
  private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

  public static void main(String[] args) throws SQLException
  {
    try {
      Class.forName(driverName);
    }
    catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(1);
    }
    Connection con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
    Statement stmt = con.createStatement();
    String tableName = "test";
    stmt.executeQuery("drop table " + tableName);
    ResultSet res = stmt.executeQuery("CREATE TABLE test (cities_and_size MAP<INT, STRING>) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n" +
"COLLECTION ITEMS TERMINATED BY '\n'  \n" +
"MAP KEYS TERMINATED BY ':'  \n" +
"LINES TERMINATED BY '\n'  \n" +
"STORED AS TEXTFILE ");
            //CREATE TABLE IF NOT EXISTS testHiveDriverTable (key INT, value STRING)");

    // show tables
    String sql = "show tables '" + tableName + "'";
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    if (res.next()) {
      System.out.println(res.getString(1));
    }
    // describe table
    sql = "describe " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1) + "\t" + res.getString(2));
    }

    // load data into table
    // NOTE: filepath has to be local to the hive server
    // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
    String filepath = "/tmp/a.txt";


    System.out.println("Running: " + sql);
    //Map<Integer,String> insertMap = new HashMap<Integer,String>();
    //insertMap.put(1, "prerna");
    sql = "load data local inpath '" + filepath + "' into table test";
    res = stmt.executeQuery(sql);

    // select * query
    sql = "select * from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1));
    }

    // regular hive query
    sql = "select count(1) from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1));

    }
  }

}
