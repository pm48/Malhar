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
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.ProcessingMode;
import static com.datatorrent.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.*;
import com.datatorrent.lib.util.TestUtils.TestInfo;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import java.io.File;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.runner.Description;

public class AbstractHiveOutputOperatorTest
{
  private static transient final Logger LOG = LoggerFactory.getLogger(AbstractHiveOutputOperatorTest.class);
  public static final String HOST_PREFIX = "jdbc:hive2://";
  public static final String HOST = "localhost";
  public static final String PORT = "10000";
  public static final String DATABASE = "default";
  public static final int NUM_WINDOWS = 10;
  public static final int BLAST_SIZE = 10;
  public static final int DATABASE_SIZE = NUM_WINDOWS * BLAST_SIZE;
  public static final String tablename = "temp";
  public static final String tablemap = "tempmap";
  public static String delimiterMap = ":";

  @Rule
  public TestInfo testMeta = new HiveTestWatcher();

  public static class HiveTestWatcher extends TestInfo
  {
    @Override
    public String getDir()
    {
      String methodName = desc.getMethodName();
      String className = desc.getClassName();
      String filePath = new File("target/" + className + "/" + methodName).getAbsolutePath();
      return filePath;
    }

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      new File(getDir()).mkdir();
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      FileUtils.deleteQuietly(new File(getDir()));
    }

  }

  public static HiveStore createStore(HiveStore hiveStore)
  {
    String host = HOST;
    String user = "prerna";
    String port = PORT;
    String password = "password";

    if (hiveStore == null) {
      hiveStore = new HiveStore();
    }

    StringBuilder sb = new StringBuilder();
    String tempHost = HOST_PREFIX + host + ":" + port;
    tempHost += "/" + DATABASE;

    LOG.debug("Host name: {}", tempHost);
    LOG.debug("User name: {}", user);
    LOG.debug("Port: {}", port);
    hiveStore.setDbUrl(tempHost);

    sb.append("user:").append(user).append(",");
    sb.append("port:").append(port).append(",");
    sb.append("password:").append(password);

    String properties = sb.toString();
    LOG.debug(properties);
    hiveStore.setConnectionProperties(properties);
    return hiveStore;
  }

  public static void hiveInitializeDatabase(HiveStore hiveStore) throws SQLException
  {
    hiveStore.connect();
    Statement stmt = hiveStore.getConnection().createStatement();
    /* ResultSet res = stmt.executeQuery("CREATE TABLE test (cities_and_size MAP<INT, STRING>) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n" +
     "COLLECTION ITEMS TERMINATED BY '\n'  \n" +
     "MAP KEYS TERMINATED BY ':'  \n" +
     "LINES TERMINATED BY '\n'  \n" +
     "STORED AS TEXTFILE ");*/
    // show tables
    String sql = "show tables";

    LOG.debug(sql);
    ResultSet res = stmt.executeQuery(sql);
    if (res.next()) {
      LOG.debug(res.getString(1));
    }

    stmt.execute("drop table " + tablename);
    //stmt.execute("CREATE TABLE IF NOT EXISTS " + tablemeta + " (dt_window bigint,dt_app_id String,dt_operator_id int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n' stored as TEXTFILE");
    stmt.execute("CREATE TABLE IF NOT EXISTS " + tablename + " (col1 int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n"
            + "STORED AS TEXTFILE ");
    /*stmt.execute("CREATE TABLE IF NOT EXISTS temp4 (col1 map<string,int>,col2 map<string,int>,col3  map<string,int>,col4 map<String,timestamp>, col5 map<string,double>,col6 map<string,double>,col7 map<string,int>,col8 map<string,int>) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  \n"
     + "COLLECTION ITEMS TERMINATED BY '\n'  \n"
     + "MAP KEYS TERMINATED BY ':'  \n"
     + "LINES TERMINATED BY '\n' "
     + "STORED AS TEXTFILE");*/

    hiveStore.disconnect();
  }

  public static void hiveInitializeMapDatabase(HiveStore hiveStore) throws SQLException
  {
    hiveStore.connect();
    Statement stmt = hiveStore.getConnection().createStatement();
    /* ResultSet res = stmt.executeQuery("CREATE TABLE test (cities_and_size MAP<INT, STRING>) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n" +
     "COLLECTION ITEMS TERMINATED BY '\n'  \n" +
     "MAP KEYS TERMINATED BY ':'  \n" +
     "LINES TERMINATED BY '\n'  \n" +
     "STORED AS TEXTFILE ");*/
    // show tables
    String sql = "show tables";

    LOG.debug(sql);
    ResultSet res = stmt.executeQuery(sql);
    if (res.next()) {
      LOG.debug(res.getString(1));
    }

    stmt.execute("drop table " + tablemap);
    //stmt.execute("CREATE TABLE IF NOT EXISTS " + tablemeta + " (dt_window bigint,dt_app_id String,dt_operator_id int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n' stored as TEXTFILE");
    stmt.execute("CREATE TABLE IF NOT EXISTS " + tablemap + " (col1 map<string,int>) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n"
            + "MAP KEYS TERMINATED BY '" + delimiterMap + "' \n"
            + "STORED AS TEXTFILE ");
    /*stmt.execute("CREATE TABLE IF NOT EXISTS temp4 (col1 map<string,int>,col2 map<string,int>,col3  map<string,int>,col4 map<String,timestamp>, col5 map<string,double>,col6 map<string,double>,col7 map<string,int>,col8 map<string,int>) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  \n"
     + "COLLECTION ITEMS TERMINATED BY '\n'  \n"
     + "MAP KEYS TERMINATED BY ':'  \n"
     + "LINES TERMINATED BY '\n' "
     + "STORED AS TEXTFILE");*/

    hiveStore.disconnect();
  }

  @Test
  public void testHiveInsertOperator() throws SQLException
  {
    hiveInitializeDatabase(createStore(null));
    HiveStore hiveStore = createStore(null);
    hiveStore.setFilepath(testMeta.getDir());
    HiveInsertOperator<String> outputOperator = new HiveInsertOperator<String>();
    outputOperator.setStore(hiveStore);
    outputOperator.setTablename(tablename);
    outputOperator.hdfsOp.setFilePermission(0777);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    outputOperator.setup(context);

    for (int wid = 0, total = 0;
            wid < NUM_WINDOWS;
            wid++) {
      outputOperator.beginWindow(wid);
      if (wid == 5) {
        outputOperator.committed(wid - 2);
      }
      for (int tupleCounter = 0;
              tupleCounter < BLAST_SIZE && total < DATABASE_SIZE;
              tupleCounter++, total++) {
        outputOperator.input.put(111 + "");
      }
      outputOperator.endWindow();

    }

    outputOperator.teardown();

    hiveStore.connect();

    int databaseSize = -1;
    Statement statement = hiveStore.getConnection().createStatement();
    ResultSet resultSet = statement.executeQuery("select count(*) from " + tablename);
    resultSet.next();
    databaseSize = resultSet.getInt(1);
    LOG.info("database size is" + databaseSize);
    hiveStore.disconnect();

    Assert.assertEquals("Numer of tuples in database",
                        33,
                        databaseSize);
  }

  @Test
  public void testHiveInsertMapOperator() throws SQLException
  {
    hiveInitializeMapDatabase(createStore(null));
    HiveStore hiveStore = createStore(null);
    hiveStore.setFilepath(testMeta.getDir());
    HiveMapInsertOperator<Map<String, Integer>> outputOperator = new HiveMapInsertOperator<Map<String, Integer>>();
    outputOperator.setStore(hiveStore);
    outputOperator.hdfsOp.setFilePermission(0777);

    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    outputOperator.setup(context);
//    outputOperator.setFilename("HiveInsertMap");
    outputOperator.setTablename(tablemap);
    outputOperator.setDelimiter(":");
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    for (int wid = 0;
            wid < NUM_WINDOWS;
            wid++) {
      outputOperator.beginWindow(wid);
      if (wid == 5) {
        outputOperator.committed(wid - 2);
      }
      //map.clear();
      for (int tupleCounter = 0;
              tupleCounter < BLAST_SIZE;
              tupleCounter++) {
        map.put(111 + "", 111);
        outputOperator.input.put(map);
        map.clear();
      }
      outputOperator.endWindow();
    }

    outputOperator.teardown();

    hiveStore.connect();

    int databaseSize = -1;

    Statement statement = hiveStore.getConnection().createStatement();
    ResultSet resultSet = statement.executeQuery("select count(*) from " + tablemap);
    resultSet.next();
    databaseSize = resultSet.getInt(1);
    LOG.info("database size is" + databaseSize);
    hiveStore.disconnect();

    Assert.assertEquals("Numer of tuples in database",
                        51,
                        databaseSize);
  }

  @Test
  public void testHDFSHiveCheckpoint() throws SQLException
  {
    hiveInitializeDatabase(createStore(null));
    HiveStore hiveStore = createStore(null);
    hiveStore.setFilepath(testMeta.getDir());
    HiveInsertOperator<String> outputOperator = new HiveInsertOperator<String>();
    HiveInsertOperator<String> newOp = new HiveInsertOperator<String>();

    outputOperator.setStore(hiveStore);
    outputOperator.hdfsOp.setFilePermission(0777);
    outputOperator.setTablename(tablename);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);
//    outputOperator.setFilename("HiveInsertMap");

    // ArrayOutOfBounds Exception seen if hdfs path is passed here.
    // outputOperator.setFilepath("hdfs://localhost:9000/user/hive");
    outputOperator.setup(context);
    for (int wid = 0, total = 0;
            wid < 10;
            wid++) {
      outputOperator.beginWindow(wid);
      if (wid == 4) {
        outputOperator.committed(wid - 1);
      }
      for (int tupleCounter = 0;
              tupleCounter < 10 && total < 100;
              tupleCounter++, total++) {
        outputOperator.processTuple(111 + "");
      }

      if (wid == 5) {
        Kryo kryo = new Kryo();
        FieldSerializer<HiveInsertOperator> f1 = (FieldSerializer<HiveInsertOperator>)kryo.getSerializer(HiveInsertOperator.class);
        FieldSerializer<HDFSRollingOutputOperator> f2 = (FieldSerializer<HDFSRollingOutputOperator>)kryo.getSerializer(HDFSRollingOutputOperator.class);
        FieldSerializer<HiveStore> f3 = (FieldSerializer<HiveStore>)kryo.getSerializer(HiveStore.class);

        f1.setCopyTransient(false);
        f2.setCopyTransient(false);
        f3.setCopyTransient(false);
        newOp = kryo.copy(outputOperator);
        //newOp.setStore(hiveStore);
        //newOp.hdfsOp.setFilePermission(0777);
        //newOp.setTablename(tablename);
      }

      if (wid == 7) {
        outputOperator.teardown();
        newOp.setup(context);

        newOp.beginWindow(5);
        for (int i = 271; i < 300; i++) {
          newOp.processTuple(111 + "");
        }
        newOp.endWindow();
        newOp.teardown();
        break;
      }

    }

    hiveStore.connect();

    int databaseSize = -1;

    Statement statement = hiveStore.getConnection().createStatement();
    ResultSet resultSet = statement.executeQuery("select count(*) from " + tablename);
    resultSet.next();
    databaseSize = resultSet.getInt(1);
    LOG.info("database size is" + databaseSize);
    Assert.assertEquals("Numer of tuples in database",
                        33,
                        databaseSize);
  }

  @Test
  public void testEmptyMaxWindows() throws SQLException
  {
    hiveInitializeDatabase(createStore(null));
    HiveStore hiveStore = createStore(null);
    hiveStore.setFilepath(testMeta.getDir());
    HiveInsertOperator<String> outputOperator = new HiveInsertOperator<String>();
    HiveInsertOperator<String> newOp = new HiveInsertOperator<String>();

    outputOperator.setStore(hiveStore);
    outputOperator.hdfsOp.setFilePermission(0777);
    outputOperator.setTablename(tablename);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);
//    outputOperator.setFilename("HiveInsertMap");

    // ArrayOutOfBounds Exception seen if hdfs path is passed here.
    // outputOperator.setFilepath("hdfs://localhost:9000/user/hive");
    outputOperator.setup(context);
    for (int wid = 0, total = 0;
            wid < 10;
            wid++) {
      outputOperator.beginWindow(wid);
      if (wid == 4) {
        outputOperator.committed(wid - 1);
      }
      for (int tupleCounter = 0;
              tupleCounter < 10 && total < 100;
              tupleCounter++, total++) {
        outputOperator.processTuple(111 + "");
      }
     outputOperator.endWindow();
    }

    for(int wid = 10; wid < 111; wid++)
    {
      outputOperator.beginWindow(wid);
      outputOperator.endWindow();
    }
    for (int wid = 111;
            wid < 120;
            wid++) {
      outputOperator.beginWindow(wid);
      if (wid == 115) {
        outputOperator.committed(wid - 1);
      }
        outputOperator.processTuple("abc");
     outputOperator.endWindow();
    }
    outputOperator.teardown();

    hiveStore.connect();

    int databaseSize = -1;

    Statement statement = hiveStore.getConnection().createStatement();
    ResultSet resultSet = statement.executeQuery("select count(*) from " + tablename);
    resultSet.next();
    databaseSize = resultSet.getInt(1);
    LOG.info("database size is" + databaseSize);
    Assert.assertEquals("Numer of tuples in database",
                        99,
                        databaseSize);
  }

}
