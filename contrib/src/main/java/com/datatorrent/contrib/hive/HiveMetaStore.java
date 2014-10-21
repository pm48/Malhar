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

import com.datatorrent.lib.db.TransactionableStore;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.validation.constraints.NotNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetaStore extends HiveStore implements TransactionableStore
{
  private static transient final Logger logger = LoggerFactory.getLogger(HiveMetaStore.class);

  public static String DEFAULT_APP_ID_COL = "dt_app_id";
  public static String DEFAULT_OPERATOR_ID_COL = "dt_operator_id";
  public static String DEFAULT_WINDOW_COL = "dt_window";
  public static String DEFAULT_META_TABLE = "dt_meta";
  public static String DEFAULT_META_FILE = "dt_file";
  protected transient FileSystem fsMeta;
  protected transient FSDataOutputStream fsMetaOutput;
  protected Statement stmtMetaInsert;
  protected Statement stmtMetaFetch;
  private boolean inTransaction;
  @NotNull
  protected String metaTableAppIdColumn;
  @NotNull
  protected String metaTableOperatorIdColumn;
  @NotNull
  protected String metaTableWindowColumn;
  @NotNull
  private String metaTable;
  @NotNull
  private String fileMeta;

  public HiveMetaStore()
  {
    super();
    metaTable = DEFAULT_META_TABLE;
    metaTableAppIdColumn = DEFAULT_APP_ID_COL;
    metaTableOperatorIdColumn = DEFAULT_OPERATOR_ID_COL;
    metaTableWindowColumn = DEFAULT_WINDOW_COL;
    fileMeta = DEFAULT_META_FILE;
    inTransaction = false;
  }

  protected void closeFile() throws IOException
  {
    /*if (bufferedOutput != null) {
     bufferedOutput.close();
     bufferedOutput = null;
     }*/
    if (fsMetaOutput != null) {
      fsMetaOutput.close();
      fsMetaOutput = null;
    }
  }

  @Override
  public void removeCommittedWindowId(String appId, int operatorId)
  {
  }

  @Override
  public void beginTransaction()
  {
  }

  @Override
  public void commitTransaction()
  {
  }

  @Override
  public void rollbackTransaction()
  {
  }

  @Override
  public boolean isInTransaction()
  {
    return false;
  }

  @Override
  public void disconnect()
  {
    if (stmtMetaInsert != null) {
      try {
        stmtMetaInsert.close();
      }
      catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    super.disconnect();
  }

  /**
   * Sets the name of the window column.<br/>
   * <b>Default:</b> {@value #DEFAULT_WINDOW_COL}
   *
   * @param windowColumn window column name.
   */
  public void setMetaTableWindowColumn(@NotNull String windowColumn)
  {
    this.metaTableWindowColumn = windowColumn;
  }

  /**
   * Sets the name of the meta table.<br/>
   * <b>Default:</b> {@value #DEFAULT_META_TABLE}
   *
   * @param metaTable meta table name.
   */
  public void setMetaTable(@NotNull String metaTable)
  {
    this.metaTable = metaTable;
  }

  @Override
  public void storeCommittedWindowId(String appId, int operatorId, long currentWindowId)
  {
    try {
      fsMeta = FileSystem.newInstance(new Path(filepath).toUri(), new Configuration());
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    Path metaFilePath = new Path(filepath + "/" + fileMeta);
    try {
      fsMetaOutput = fsMeta.create(metaFilePath);
    }
    catch (IOException ex) {
      logger.info(HiveMetaStore.class.getName() + ex);
    }
    int currentWindow = (int)currentWindowId;
    try {
      fsMetaOutput.writeInt(currentWindow);
      fsMetaOutput.write(appId.getBytes());
      fsMetaOutput.writeByte(operatorId);
    }
    catch (IOException ex) {
      logger.info(HiveMetaStore.class.getName() + ex);
    }
    try {
      if (fsMetaOutput != null) {
        fsMetaOutput.close();
        fsMetaOutput = null;
      }
    }
    catch (IOException ex) {
      logger.debug(HiveMetaStore.class.getName() + ex);
    }

    try {
      stmtMetaInsert = getConnection().createStatement();
      stmtMetaInsert.execute("load data inpath '" + metaFilePath + "' into table " + metaTable);
      ResultSet res = stmtMetaInsert.executeQuery("select * from temp");
      while (res.next()) {
        logger.info("got result" + res.getString(1));
      }

    }
    catch (SQLException ex) {
      logger.info(HiveMetaStore.class.getName() + ex.getCause());
    }

  }

  @Override
  public long getCommittedWindowId(String appId, int operatorId)
  {
    Long lastWindow = getCommittedWindowIdHelper(appId, operatorId);

    try {
      if (stmtMetaFetch != null) {
        stmtMetaFetch.close();
      }
      logger.debug("Last window id: {}", lastWindow);

      if (lastWindow == null) {
        return -1L;
      }
      else {
        return lastWindow;
      }
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * This is a helper method for loading the committed window Id.
   *
   * @param appId The application ID.
   * @param operatorId The operator ID.
   * @return The last committed window. If there is no previously committed window this will return null.
   */
  protected Long getCommittedWindowIdHelper(String appId, int operatorId)
  {
    try {
      String command = "select " + metaTableWindowColumn + " from " + metaTable + " where " + metaTableAppIdColumn
              + " = '" + appId + "' and " + metaTableOperatorIdColumn + " = " + operatorId;

      logger.debug(command);
      stmtMetaFetch = getConnection().createStatement();
      Long lastWindow = null;
      ResultSet resultSet = stmtMetaFetch.executeQuery(command);
      logger.info("Got result");
      int lastWindowInt = -1;
      while (resultSet.next()) {
        lastWindowInt = resultSet.getInt(1);
        logger.info("In resultset next method");
        lastWindow = (long)lastWindowInt;
        logger.debug("last window is" + lastWindow);
        logger.info("last integer window is" + lastWindowInt);
      }
      return lastWindow;
    }
    catch (SQLException ex) {
      logger.info("Exception occurred" + ex.getMessage());
      throw new RuntimeException(ex);
    }
  }

}
