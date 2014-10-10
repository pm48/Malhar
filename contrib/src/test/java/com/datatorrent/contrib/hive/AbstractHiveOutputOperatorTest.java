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

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.ProcessingMode;
import static com.datatorrent.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.APP_ID;
import static com.datatorrent.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.OPERATOR_ID;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import java.sql.*;
import java.util.Random;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractHiveOutputOperatorTest
{
  private static transient final Logger LOG = LoggerFactory.getLogger(AbstractHiveOutputOperatorTest.class);
  public static final String HOST_PREFIX = "jdbc:hive://";
  public static final String HOST = "localhost";
  public static final String PORT = "10000";
  public static final String DATABASE = "default";
  public static final String table = "test";
  public static final int NUM_WINDOWS = 10;
  public static final int BLAST_SIZE = 10;
  public static final int DATABASE_SIZE = NUM_WINDOWS * BLAST_SIZE;
  public static final int BATCH_SIZE = DATABASE_SIZE / 5;

  public static HiveStore createStore(HiveStore hiveStore)
  {
    String host = HOST;
    String user = "";
    String port = PORT;
    String password ="";

    if(hiveStore == null) {
      hiveStore = new HiveStore();
    }

    StringBuilder sb = new StringBuilder();
    String tempHost = HOST_PREFIX + host + ":" + PORT;
    tempHost += "/" + DATABASE;

    LOG.debug("Host name: {}", tempHost);
    LOG.debug("User name: {}", user);
    LOG.debug("Port: {}" , port);
    hiveStore.setDbUrl(tempHost);

    sb.append("user:").append(user).append(",");
    sb.append("port:").append(port);
    sb.append("password:").append("");

    String properties = sb.toString();
    LOG.debug(properties);
    hiveStore.setConnectionProperties(properties);
    return hiveStore;
  }

  public static void hiveInitializeDatabase(HiveStore hiveStore) throws SQLException
  {
    hiveStore.connect();
    Statement stmt = hiveStore.getConnection().createStatement();
    stmt.executeQuery("drop table " + table);
  /* ResultSet res = stmt.executeQuery("CREATE TABLE test (cities_and_size MAP<INT, STRING>) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n" +
"COLLECTION ITEMS TERMINATED BY '\n'  \n" +
"MAP KEYS TERMINATED BY ':'  \n" +
"LINES TERMINATED BY '\n'  \n" +
"STORED AS TEXTFILE ");*/

            //CREATE TABLE IF NOT EXISTS testHiveDriverTable (key INT, value STRING)");

    // show tables
   // String sql = "show tables '" + table + "'";
   stmt.executeQuery("drop table temp");
    stmt.executeQuery("Create table temp (key int,value string) stored as TEXTFILE");
 String sql = "update table temp set key=? value=?";
   PreparedStatement prepstmt =hiveStore.getConnection().prepareStatement(sql);

    prepstmt.setInt(1, 20);
		prepstmt.setString(2, "prerna");
     ResultSet res = prepstmt.executeQuery();
    if (res.next()) {
      System.out.println(res.getString(1));
    }

    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    if (res.next()) {
      System.out.println(res.getString(1));
    }
    // describe table
    sql = "describe " + table;
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
    sql = "select * from " + table;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1));
    }

    // regular hive query
    sql = "select count(1) from " + table;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1));

    }


    stmt.close();

    hiveStore.disconnect();
  }

  public static void cleanDatabase() throws SQLException
  {
     hiveInitializeDatabase(createStore(null));
  }

  @Test
  public void testHiveOutputOperator() throws SQLException
  {
    cleanDatabase();
    HiveStore hiveStore = createStore(null);

    Random random = new Random();
    HiveOutputOperator outputOperator = new HiveOutputOperator();

    outputOperator.setStore(hiveStore);
    outputOperator.setBatchSize(BATCH_SIZE);

    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    outputOperator.setup(context);

    for(int wid = 0, total = 0;
        wid < NUM_WINDOWS;
        wid++) {
      outputOperator.beginWindow(wid);

      for(int tupleCounter = 0;
          tupleCounter < BLAST_SIZE && total < DATABASE_SIZE;
          tupleCounter++,
          total++) {
       // outputOperator.input.put(random.nextInt());
      }

      outputOperator.endWindow();
    }

    outputOperator.teardown();

    hiveStore.connect();

    int databaseSize = -1;

    Statement statement = hiveStore.getConnection().createStatement();
    ResultSet resultSet = statement.executeQuery("select count(*) from test");
    resultSet.next();
    databaseSize = resultSet.getInt(1);

    hiveStore.disconnect();

    Assert.assertEquals("Numer of tuples in database",
                        DATABASE_SIZE,
                        databaseSize);
  }
}
