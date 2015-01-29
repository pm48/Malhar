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
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.contrib.hive.FSRollingOutputOperator.FilePartitionMapping;
import static com.datatorrent.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.*;
import com.datatorrent.lib.util.TestUtils.TestInfo;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.*;
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
  public static final String tablename = "hivetable";
  public static final String tablemap = "tempmap";
  public static String delimiterMap = ":";
  private static int OPERATOR_ID = 0;

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
      LOG.debug("filepath is {}", filePath);
      return filePath;
    }

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      new File(getDir()).mkdir();
    }

   /* @Override
    protected void finished(Description description)
    {
      super.finished(description);
      FileUtils.deleteQuietly(new File(getDir()));
    }*/

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
    stmt.execute("CREATE TABLE IF NOT EXISTS " + tablename + " (col1 string) PARTITIONED BY(dt STRING,country STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n"
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
    stmt.execute("CREATE TABLE IF NOT EXISTS " + tablemap + " (col1 map<string,int>) PARTITIONED BY(dt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n"
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
    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    hivePartitionColumns.add("country");
    hiveInitializeDatabase(createStore(null));
    HiveStore hiveStore = createStore(null);
    hiveStore.setFilepath(testMeta.getDir());
    HiveOperator hiveOperator = new HiveOperator();
    hiveOperator.setStore(hiveStore);
    hiveOperator.setTablename(tablename);
    hiveOperator.setHivePartitionColumns(hivePartitionColumns);
    hiveOperator.setHivepath("/user/hive/warehouse");

    FSRollingOutputOperator<String> fsRolling = new FSRollingOutputOperator<String>();
    fsRolling.setFilePath(testMeta.getDir() );
    fsRolling.setFilePermission(0777);
    fsRolling.setMaxLength(128);
    HivePartition<String> partition = new HivePartition<String>();
    fsRolling.setHivePartition(partition);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    fsRolling.setup(context);
    hiveOperator.setup(context);
    fsRolling.setConverter(new StringConverter());
    FilePartitionMapping mapping1 = new FilePartitionMapping();
    FilePartitionMapping mapping2 = new FilePartitionMapping();
    for (int wid = 0, total = 0;
            wid < NUM_WINDOWS;
            wid++) {
      fsRolling.beginWindow(wid);
      for (int tupleCounter = 0;
              tupleCounter < BLAST_SIZE && total < DATABASE_SIZE;
              tupleCounter++, total++) {
        fsRolling.input.put(111 + "");
        fsRolling.input.put(222 + "");
      }
      if (wid == 7) {
        fsRolling.committed(wid - 1);
        mapping1.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "111" + "/" + "0-transactions.out.part.0");
        ArrayList<String> partitions = new ArrayList<String>();
       partitions.add("111");
      // partitions.add("222");
        mapping1.setPartition(partitions);
        hiveOperator.processTuple(mapping1);
        //mapping2.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "111" + "/" + "0-transactions.out.part.1");
        //mapping2.setPartition(partitions);
       // hiveOperator.processTuple(mapping2);
      }

      fsRolling.endWindow();

    }

    //outputOperator.teardown();
    hiveStore.connect();

    int databaseSize = -1;
    Statement statement = hiveStore.getConnection().createStatement();
    ResultSet resultSet = statement.executeQuery("select count(*) from " + tablename);
    resultSet.next();
    databaseSize = resultSet.getInt(1);
    LOG.debug("database size is {}", databaseSize);
    hiveStore.disconnect();

    Assert.assertEquals("Numer of tuples in database",
                        66,
                        databaseSize);
  }

  @Test
  public void testHiveInsertMapOperator() throws SQLException
  {
    HiveOperator hiveOperator = new HiveOperator();
    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    hiveOperator.setHivePartitionColumns(hivePartitionColumns);

    hiveInitializeMapDatabase(createStore(null));
    HiveStore hiveStore = createStore(null);
    hiveStore.setFilepath(testMeta.getDir());
    FSRollingOutputOperator<Map<String, Object>> fsRolling = new FSRollingOutputOperator<Map<String, Object>>();
    hiveOperator.setStore(hiveStore);
    fsRolling.setFilePath(testMeta.getDir());
    fsRolling.setFilePermission(0777);
    fsRolling.setMaxLength(128);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    fsRolling.setup(context);
    hiveOperator.setup(context);
    MapConverter converter = new MapConverter();
    converter.setDelimiter(":");
    fsRolling.setConverter(converter);
    HiveMapPartition partition = new HiveMapPartition();
    fsRolling.setHivePartition(partition);
    hiveOperator.setTablename(tablemap);
    //fsRolling.setDelimiter(":");
    HashMap<String, Object> map = new HashMap<String, Object>();
    FilePartitionMapping mapping1 = new FilePartitionMapping();
    FilePartitionMapping mapping2 = new FilePartitionMapping();

    for (int wid = 0;
            wid < NUM_WINDOWS;
            wid++) {
      fsRolling.beginWindow(wid);
      for (int tupleCounter = 0;
              tupleCounter < BLAST_SIZE;
              tupleCounter++) {
        map.put(111 + "", 111);
        fsRolling.input.put(map);
        map.clear();
      }

      if (wid == 6) {
        fsRolling.committed(wid - 2);
        mapping1.setFilename("0-transactions.out.part.0");
//        mapping1.setPartition("111");
        hiveOperator.processTuple(mapping1);
        mapping2.setFilename("0-transactions.out.part.1");
   //     mapping2.setPartition("111");
        hiveOperator.processTuple(mapping2);
      }

      fsRolling.endWindow();
    }

    fsRolling.teardown();

    hiveStore.connect();

    int databaseSize = -1;

    Statement statement = hiveStore.getConnection().createStatement();
    ResultSet resultSet = statement.executeQuery("select count(*) from " + tablemap);
    resultSet.next();
    databaseSize = resultSet.getInt(1);
    LOG.debug("database size is {}", databaseSize);
    hiveStore.disconnect();

    Assert.assertEquals("Numer of tuples in database",
                        34,
                        databaseSize);
  }

  @Test
  public void testHivePartitions() throws SQLException
  {
    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    HiveOperator hiveOperator = new HiveOperator();
    FSRollingOutputOperator<String> fsRolling = new FSRollingOutputOperator<String>();
    fsRolling.setConverter(new StringConverter());
    hiveInitializeDatabase(createStore(null));
    HiveStore hiveStore = createStore(null);
    hiveStore.setFilepath(testMeta.getDir());
    hiveOperator.setHivePartitionColumns(hivePartitionColumns);
    //RandomWordGenerator randomGenerator = new RandomWordGenerator();
    hiveOperator.setStore(hiveStore);
    hiveOperator.setTablename(tablename);
    fsRolling.setFilePath(testMeta.getDir());
    fsRolling.setFilePermission(0777);
    fsRolling.setMaxLength(128);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    fsRolling.setup(context);
    hiveOperator.setup(context);
    HivePartition<String> partition = new HivePartition<String>();
    fsRolling.setHivePartition(partition);

    List<Partition<HiveOperator>> partitions = Lists.newArrayList();

    partitions.add(new DefaultPartition<HiveOperator>(hiveOperator));

   /* Collection<Partition<HiveOperator>> newPartitions = hiveOperator.definePartitions(partitions, 1);
  //  LOG.debug("newpartitions size is {}", newPartitions.size());

  //  for (Partition<HiveOperator> p: newPartitions) {
      Assert.assertNotSame(hiveOperator, p.getPartitionedInstance());

    }
    /* Collect all operators in a list
    List<HiveOperator> opers = Lists.newArrayList();
    for (Partition<HiveOperator> p: newPartitions) {
      HiveOperator oi = p.getPartitionedInstance();
      oi.setup(context);
      oi.setTablename(tablename);
      opers.add(oi);
      OPERATOR_ID++;
    }

    int wid = 0;
    int j = 0;
    int total = 0;
    FilePartitionMapping mapping1 = new FilePartitionMapping();

    for (int i = 0; i < 10; i++) {
      fsRolling.beginWindow(wid);
      for (int tupleCounter = 0;
              tupleCounter < BLAST_SIZE && total < DATABASE_SIZE;
              tupleCounter++, total++) {
        fsRolling.input.put(111 + "");
      }
      for (HiveOperator o: opers) {
        //o.beginWindow(wid);
        if (wid == 6) {
          fsRolling.committed(wid - 2);
          mapping1.setFilename("0-transactions.out.part." + j);
          mapping1.setPartition("111");
          o.processTuple(mapping1);
          j++;
        }
        // o.endWindow();
      }
      wid++;
    }
    fsRolling.endWindow();

    for (HiveOperator o: opers) {
      o.endWindow();
      o.teardown();
    }
    hiveStore.connect();

    int databaseSize = -1;
    Statement statement = hiveStore.getConnection().createStatement();
    ResultSet resultSet = statement.executeQuery("select count(*) from " + tablename);
    resultSet.next();
    databaseSize = resultSet.getInt(1);
    LOG.debug("database size is {}" , databaseSize);
    hiveStore.disconnect();

    Assert.assertEquals("Numer of tuples in database",
                        66,
                        databaseSize);*/
  }

  @Test
  public void testHDFSHiveCheckpoint() throws SQLException
  {
    hiveInitializeDatabase(createStore(null));
    HiveStore hiveStore = createStore(null);
    hiveStore.setFilepath(testMeta.getDir());
    HiveOperator outputOperator = new HiveOperator();
    HiveOperator newOp = new HiveOperator();

    outputOperator.setStore(hiveStore);
    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    FSRollingOutputOperator<String> fsRolling = new FSRollingOutputOperator<String>();
    fsRolling.setConverter(new StringConverter());
    hiveInitializeDatabase(createStore(null));
    outputOperator.setHivePartitionColumns(hivePartitionColumns);
    //RandomWordGenerator randomGenerator = new RandomWordGenerator();
    outputOperator.setStore(hiveStore);
    outputOperator.setTablename(tablename);
    fsRolling.setFilePath(testMeta.getDir());
    fsRolling.setFilePermission(0777);
    fsRolling.setMaxLength(128);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    fsRolling.setup(context);
    FilePartitionMapping mapping1 = new FilePartitionMapping();
    FilePartitionMapping mapping2 = new FilePartitionMapping();
    FilePartitionMapping mapping3 = new FilePartitionMapping();
    outputOperator.setup(context);
    HivePartition<String> partition = new HivePartition<String>();
    fsRolling.setHivePartition(partition);
    for (int wid = 0, total = 0;
            wid < 20;
            wid++) {
      fsRolling.beginWindow(wid);
      for (int tupleCounter = 0;
              tupleCounter < 10 && total < 100;
              tupleCounter++, total++) {
        fsRolling.input.process(123 + "");
      }
      if (wid == 6) {
        fsRolling.committed(wid - 1);
        mapping1.setFilename("0-transactions.out.part.0");
//        mapping1.setPartition("123");
        outputOperator.processTuple(mapping1);
      }
      if (wid == 15) {
        fsRolling.committed(14);
        mapping2.setFilename("0-transactions.out.part.1");
   //     mapping2.setPartition("123");
        outputOperator.processTuple(mapping2);
      }

      if (wid == 18) {
        Kryo kryo = new Kryo();
        FieldSerializer<HiveOperator> f1 = (FieldSerializer<HiveOperator>)kryo.getSerializer(HiveOperator.class);
        FieldSerializer<HiveStore> f2 = (FieldSerializer<HiveStore>)kryo.getSerializer(HiveStore.class);

        f1.setCopyTransient(false);
        f2.setCopyTransient(false);
        newOp = kryo.copy(outputOperator);
        outputOperator.teardown();
        newOp.setup(context);

        newOp.beginWindow(15);
        mapping3.setFilename("0-transactions.out.part.2");
//        mapping3.setPartition("123");

        newOp.processTuple(mapping3);

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
    LOG.debug("database size is {}", databaseSize);
    Assert.assertEquals("Numer of tuples in database",
                        99,
                        databaseSize);
  }

}
