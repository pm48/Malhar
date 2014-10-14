/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.hive;

import com.datatorrent.lib.db.TransactionableStore;
import static com.datatorrent.lib.db.jdbc.JdbcTransactionalStore.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HiveMetaStore extends HiveStore implements TransactionableStore
{
  private static transient final Logger LOG = LoggerFactory.getLogger(HiveMetaStore.class);

  public static String DEFAULT_APP_ID_COL = "dt_app_id";
  public static String DEFAULT_OPERATOR_ID_COL = "dt_operator_id";
  public static String DEFAULT_WINDOW_COL = "dt_window";
  public static String DEFAULT_META_TABLE = "dt_meta";
  private boolean inTransaction;
  @NotNull
  protected String metaTableAppIdColumn;
  @NotNull
  protected String metaTableOperatorIdColumn;
  @NotNull
  protected String metaTableWindowColumn;
  @NotNull
  private String metaTable;

  public HiveMetaStore(){
    super();
    metaTable = DEFAULT_META_TABLE;
    metaTableAppIdColumn = DEFAULT_APP_ID_COL;
    metaTableOperatorIdColumn = DEFAULT_OPERATOR_ID_COL;
    metaTableWindowColumn = DEFAULT_WINDOW_COL;
    inTransaction = false;
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

  public void storeCommittedWindowId(String appId, int operatorId, long currentWindowId)
  {
    /* try {
      lastWindowUpdateCommand.setLong(1, currentWindowId);
      lastWindowUpdateCommand.setString(2, appId);
      lastWindowUpdateCommand.setInt(3, operatorId);
      lastWindowUpdateCommand.executeUpdate();
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }*/
  }

    @Override
  public long getCommittedWindowId(String appId, int operatorId)
  {
    Long lastWindow = getCommittedWindowIdHelper(appId, operatorId);

    try {
      if(lastWindow == null) {
//        lastWindowInsertCommand.close();
        connection.commit();
      }

//      lastWindowFetchCommand.close();
      LOG.debug("Last window id: {}", lastWindow);

      if(lastWindow == null) {
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
   * @param appId The application ID.
   * @param operatorId The operator ID.
   * @return The last committed window. If there is no previously committed window this will return null.
   */
  protected Long getCommittedWindowIdHelper(String appId, int operatorId)
  {
/*    try {
      lastWindowFetchCommand.setString(1, appId);
//      lastWindowFetchCommand.setInt(2, operatorId);
      Long lastWindow = null;
//      ResultSet resultSet = lastWindowFetchCommand.executeQuery();
   /*   if (resultSet.next()) {
        lastWindow = resultSet.getLong(1);
      }
      else {
  //      lastWindowInsertCommand.setString(1, appId);
  //      lastWindowInsertCommand.setInt(2, operatorId);
  //      lastWindowInsertCommand.setLong(3, -1);
  //      lastWindowInsertCommand.executeUpdate();
      }
      return lastWindow;
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }*/
    return null;
  }



}
