/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.db.AbstractStoreOutputOperator;
import com.google.common.collect.Lists;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHiveOutputOperator<T,S extends HiveStore> extends AbstractStoreOutputOperator<T, HiveStore>
{
  protected static int DEFAULT_BATCH_SIZE = 1000;

  @Min(1)
  private int batchSize;
  private final List<T> tuples;

  private transient int batchStartIdx;
  private transient PreparedStatement updateCommand;
  private static final Logger logger = LoggerFactory.getLogger(AbstractHiveOutputOperator.class);
  protected long tupleCount = 0;
  private ProcessingMode mode;
  private transient long currentWindowId;
  private transient long committedWindowId;
  private transient String appId;
  private transient int operatorId;
  @NotNull
  private String tableName = "";

  public String getTableName()
  {
    return tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  public AbstractHiveOutputOperator()
  {
    tuples = Lists.newArrayList();
    batchSize = DEFAULT_BATCH_SIZE;
    batchStartIdx = 0;
    this.setStore((S) new HiveStore());
  /*  try {
      updateCommand = store.getConnection().prepareStatement(getUpdateCommand());
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }*/

  }

  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public int getBatchSize()
  {
    return batchSize;
  }

  public void setMode(ProcessingMode mode)
  {
    this.mode = mode;
  }

  public ProcessingMode getMode()
  {
    return mode;
  }

  public String getAppId()
  {
    return appId;
  }

  public int getOperatorId()
  {
    return operatorId;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
   try {
      updateCommand = store.getConnection().prepareStatement(getUpdateCommand());
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }

    mode = context.getValue(OperatorContext.PROCESSING_MODE);

    if (mode == ProcessingMode.AT_MOST_ONCE) {
      //Batch must be cleared to avoid writing same data twice
      tuples.clear();
    }

  try {
      for (T tempTuple: tuples) {
        //setStatementParameters(updateCommand, tempTuple);
        updateCommand.addBatch();
      }
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }

    appId = context.getValue(DAG.APPLICATION_ID);
    operatorId = context.getId();


    logger.debug("AppId {} OperatorId {}", appId, operatorId);
    logger.debug("Committed window id {}", committedWindowId);

  }

  /*@Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    try {
      updateCommand = store.getConnection().prepareStatement(getUpdateCommand());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }*/


 /* public void processTuple(T tuple)
  {
    try {
      //setStatementParameters(updateCommand, tuple);
      updateCommand.execute();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }*/
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.currentWindowId = windowId;
    logger.debug("Committed window {}, current window {}", committedWindowId, currentWindowId);
  }

  @Override
  public void endWindow()
  {
    if (tuples.size() - batchStartIdx > 0) {
      processBatch();
    }
    //This window is done so write it to the database.
    if (committedWindowId < currentWindowId) {
//      store.storeCommittedWindowId(appId, operatorId, currentWindowId);
      committedWindowId = currentWindowId;
    }
    super.endWindow();
    tuples.clear();
    batchStartIdx = 0;
  }

  @Override
  public void processTuple(T tuple)
  {
    if (committedWindowId >= currentWindowId) {
      return;
    }
    tuples.add(tuple);
    if ((tuples.size() - batchStartIdx) >= batchSize) {
      processBatch();
    }
  }

  private void processBatch()
  {
    logger.debug("start {} end {}", batchStartIdx, tuples.size());
    try {
       for (int i = batchStartIdx; i < tuples.size(); i++) {
       // setStatementParameters(updateCommand, tuples.get(i));
        updateCommand.addBatch();
      }
      updateCommand.executeBatch();
      updateCommand.clearBatch();
    }
    catch (SQLException e) {
      throw new RuntimeException("processing batch", e);
    }
    finally {
      batchStartIdx += tuples.size() - batchStartIdx;
    }
  }



  /**
   * Gets the statement which insert/update the table in the database.
   *
   * @return the sql statement to update a tuple in the database.
   */
  @Nonnull
  protected abstract String getUpdateCommand();

  /**
   * Sets the parameter of the insert/update statement with values from the tuple.
   *
   * @param statement update statement which was returned by {@link #getUpdateCommand()}
   * @param tuple tuple
   * @throws SQLException
   */
  //protected abstract void setStatementParameters(PreparedStatement statement, T tuple) throws SQLException;

}
