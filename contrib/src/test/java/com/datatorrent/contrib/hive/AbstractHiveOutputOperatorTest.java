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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractHiveOutputOperatorTest
{
  private static transient final Logger LOG = LoggerFactory.getLogger(AbstractHiveOutputOperatorTest.class);
  public static final String HOST_PREFIX = "jdbc:hive2://";
  public static final String HOST = "localhost";
  public static final String PORT = "10000";
  public static final String DATABASE = "default";
  public static final String table = "dt_meta";
  public static final int NUM_WINDOWS = 10;
  public static final int BLAST_SIZE = 10;
  public static final int DATABASE_SIZE = NUM_WINDOWS * BLAST_SIZE;
  public static final int BATCH_SIZE = DATABASE_SIZE / 5;

  public static HiveMetaStore createStore(HiveMetaStore hiveStore)
  {
    String host = HOST;
    String user = "";
    String port = PORT;
    String password = "";

    if (hiveStore == null) {
      hiveStore = new HiveMetaStore();
    }

    StringBuilder sb = new StringBuilder();
    String tempHost = HOST_PREFIX + host + ":" + PORT;
    tempHost += "/" + DATABASE;

    LOG.debug("Host name: {}", tempHost);
    LOG.debug("User name: {}", user);
    LOG.debug("Port: {}", port);
    hiveStore.setDbUrl(tempHost);

    sb.append("user:").append(user).append(",");
    sb.append("port:").append(port);
    sb.append("password:").append("");

    String properties = sb.toString();
    LOG.debug(properties);
    hiveStore.setConnectionProperties(properties);
    return hiveStore;
  }

  public static void hiveInitializeDatabase(HiveMetaStore hiveStore) throws SQLException
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
    stmt.execute("drop table dt_meta");
    stmt.execute("drop table tempMap");
    stmt.execute("drop table temp");
    stmt.execute("Create table  IF NOT EXISTS dt_meta (dt_window int,dt_app_id String,dt_operator_id int) stored as TEXTFILE");
    stmt.execute("CREATE TABLE IF NOT EXISTS tempMap (col1 map<string,int>) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n"
            + "COLLECTION ITEMS TERMINATED BY '\n'  \n"
            + "MAP KEYS TERMINATED BY ':'  \n"
            + "LINES TERMINATED BY '\n'  \n"
            + "STORED AS TEXTFILE ");
    stmt.execute("CREATE TABLE IF NOT EXISTS temp (col1 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n"
            + "COLLECTION ITEMS TERMINATED BY '\n'  \n"
            + "LINES TERMINATED BY '\n'  \n"
            + "STORED AS TEXTFILE ");
    /*stmt.execute("CREATE TABLE IF NOT EXISTS temp4 (col1 map<string,int>,col2 map<string,int>,col3  map<string,int>,col4 map<String,timestamp>, col5 map<string,double>,col6 map<string,double>,col7 map<string,int>,col8 map<string,int>) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  \n"
     + "COLLECTION ITEMS TERMINATED BY '\n'  \n"
     + "MAP KEYS TERMINATED BY ':'  \n"
     + "LINES TERMINATED BY '\n' "
     + "STORED AS TEXTFILE");*/

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
  }

}
