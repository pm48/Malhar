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

    Connection con = DriverManager.getConnection("jdbc:hive2://localhost:9000/default", "", "");
    Statement stmt = con.createStatement();
    String tableName = "temp";
   //stmt.executeQuery("drop table " + tableName);
    stmt.execute("CREATE TABLE IF NOT EXISTS temp (cities_and_size STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n" +
"COLLECTION ITEMS TERMINATED BY '\n'  \n" +
"MAP KEYS TERMINATED BY ':'  \n" +
"LINES TERMINATED BY '\n'  \n" +
"STORED AS TEXTFILE ");
            //CREATE TABLE IF NOT EXISTS testHiveDriverTable (key INT, value STRING)");

    // show tables
    String sql = "show tables '" + tableName + "'";
    System.out.println("Running: " + sql);
    stmt.execute(sql);
   /* if (res.next()) {
      System.out.println(res.getString(1));
    }*/
    // describe table
    sql = "describe temp";
    System.out.println("Running: " + sql);
    stmt.execute(sql);
    /*while (res.next()) {
      System.out.println(res.getString(1) + "\t" + res.getString(2));
    }*/

    // load data into table
    // NOTE: filepath has to be local to the hive server
    // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
    String filepath = "hdfs://localhost:9000/user/b.txt";


    System.out.println("Running: " + filepath);
    //Map<Integer,String> insertMap = new HashMap<Integer,String>();
    //insertMap.put(1, "prerna");
    sql = "load data inpath '/user/wordsforproblem.txt' INTO TABLE temp";
    stmt.execute(sql);

    // select * query
    sql = "select * from temp";
    System.out.println("Running: " + sql);
    stmt.execute(sql);
     /*while (res.next()) {
      System.out.println(res.getString(1));
    }*/

    // regular hive query
    sql = "select count(1) from " + tableName;
    System.out.println("Running: " + sql);
    stmt.execute(sql);
   /* while (res.next()) {
      System.out.println(res.getString(1));

    }*/
  }

}
