/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hive;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * A standalone program to test Hive
 */
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
