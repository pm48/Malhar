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

import com.datatorrent.api.*;
import java.sql.SQLException;
import java.sql.Statement;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.db.AbstractStoreOutputOperator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.common.util.DTThrowable;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import javax.validation.constraints.Min;
import org.apache.hadoop.fs.Path;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.*;
import org.apache.commons.io.output.ByteArrayOutputStream;

/*
 * An abstract Hive operator which can insert data in txt format in tables/partitions from a file written in hdfs location.
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public abstract class AbstractHiveHDFS<T> extends AbstractStoreOutputOperator<T, HiveStore> implements CheckpointListener, Partitioner<AbstractHiveHDFS<T>>
{
  protected transient boolean isHivePartitioned = true;
  protected transient int numPartitions = 2;
  //This Property is user configurable.
  protected ArrayList<String> hivePartitionColumns = new ArrayList<String>();

  public ArrayList<String> getHivePartitionColumns()
  {
    return hivePartitionColumns;
  }

  public void setHivePartitionColumns(ArrayList<String> hivePartitionColumns)
  {
    this.hivePartitionColumns = hivePartitionColumns;
  }

  protected HashMap<String, String> mapFilePartition = new HashMap<String, String>();
  protected String partition;
  @Nonnull
  protected String tablename;

  public HDFSRollingOutputOperator<T> hdfsOp;
  //This variable is user configurable.
  @Min(0)
  private transient long maxWindowsWithNoData = 100;

  public int getNumPartitions()
  {
    return numPartitions;
  }

  public void setNumPartitions(int numPartitions)
  {
    this.numPartitions = numPartitions;
  }

  @Override
  public Collection<Partition<AbstractHiveHDFS<T>>> definePartitions(Collection<Partition<AbstractHiveHDFS<T>>> partitions, int incrementalCapacity)
  {
    int totalCount = numPartitions;
    Collection<Partition<AbstractHiveHDFS<T>>> newPartitions = Lists.newArrayListWithExpectedSize(totalCount);
    Kryo kryo = new Kryo();
    for (int i = 0; i < totalCount; i++) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      kryo.writeObject(output, this);
      output.close();
      Input lInput = new Input(bos.toByteArray());
      @SuppressWarnings("unchecked")
      AbstractHiveHDFS<T> oper = kryo.readObject(lInput, this.getClass());
      newPartitions.add(new DefaultPartition<AbstractHiveHDFS<T>>(oper));
    }
    // assign the partition keys
    DefaultPartition.assignPartitionKeys(newPartitions, input);

    return newPartitions;

  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractHiveHDFS<T>>> partitions)
  {
  }

  protected long committedWindowId = Stateless.WINDOW_ID;
  private static final Logger logger = LoggerFactory.getLogger(AbstractHiveHDFS.class);
  private transient String appId;
  private transient int operatorId;
  protected HashMap<String, Long> mapFilenames;

  public long getMaxWindowsWithNoData()
  {
    return maxWindowsWithNoData;
  }

  public void setMaxWindowsWithNoData(long maxWindowsWithNoData)
  {
    this.maxWindowsWithNoData = maxWindowsWithNoData;
  }

  private int countEmptyWindow;
  private boolean isEmptyWindow;
  protected long windowIDOfCompletedPart = Stateless.WINDOW_ID;

  public HDFSRollingOutputOperator<T> getHdfsOp()
  {
    return hdfsOp;
  }

  public void setHdfsOp(HDFSRollingOutputOperator<T> hdfsOp)
  {
    this.hdfsOp = hdfsOp;
  }

  public AbstractHiveHDFS()
  {
    hdfsOp = new HDFSRollingOutputOperator<T>();
    mapFilenames = new HashMap<String, Long>();
    hdfsOp.hive = this;
    countEmptyWindow = 0;
  }

  @Override
  public void beginWindow(long windowId)
  {
    isEmptyWindow = true;
    windowIDOfCompletedPart = windowId;
  }

  /*
   * Moving completed files into hive on committed window callback.
   * Criteria for moving them is that the windowId in which they are completed
   * should be less than committed window.
   */
  @Override
  public void committed(long windowId)
  {
    committedWindowId = windowId;
    Iterator<String> iter = mapFilenames.keySet().iterator();
    while (iter.hasNext()) {
      String fileMoved = iter.next();
      long window = mapFilenames.get(fileMoved);
      logger.info("file to be moved is {}", fileMoved);
      logger.info("window is {}", window);
      if (committedWindowId >= window) {
        try {
          logger.info("path in committed window is" + store.getOperatorpath() + fileMoved);
          /*
           * Check if file was not moved to hive because of operator crash or any other failure.
           * When FSWriter comes back to the checkpointed state, it would check for this file and then move it to hive.
           */
          if (hdfsOp.getFileSystem().exists(new Path(store.getOperatorpath() + fileMoved))) {
            processHiveFile(fileMoved);
          }
        }
        catch (IOException ex) {
          logger.debug(ex.getMessage());
        }
        iter.remove();
      }

    }
  }

  /*
   *This method can be used for debugging purposes.
   */
  @Override
  public void checkpointed(long windowId)
  {
  }

  public String getTablename()
  {
    return tablename;
  }

  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  /**
   * Function to process each incoming tuple
   * This can be overridden by user for multiple partition columns.
   * Giving an implementation for one partition column.
   * @param tuple incoming tuple
   */
  @Override
  public void processTuple(T tuple)
  {
    if (isHivePartitioned) {
      partition = getHivePartition(tuple);
    }
    partition = getHivePartitionColumns().get(0) + "=" + partition;
    isEmptyWindow = false;
    hdfsOp.input.process(tuple);
  }

  @Override
  public void setup(OperatorContext context)
  {
    appId = context.getValue(DAG.APPLICATION_ID);
    operatorId = context.getId();
    hdfsOp.setFilePath(store.filepath + "/" + appId + "/" + operatorId);
    store.setOperatorpath(store.filepath + "/" + appId + "/" + operatorId);
    super.setup(context);
    hdfsOp.setup(context);
    isEmptyWindow = true;
  }

  @Override
  public void teardown()
  {
    hdfsOp.teardown();
    super.teardown();
  }

  @Override
  public void endWindow()
  {
    if (isEmptyWindow) {
      countEmptyWindow++;
    }
    if (countEmptyWindow >= maxWindowsWithNoData) {
      logger.debug("empty window count is max.");
      String lastFile = hdfsOp.getHDFSRollingLastFile();
      try {
        logger.debug("path in end window is" + store.getOperatorpath() + "/" + lastFile);
        if (hdfsOp.getFileSystem().exists(new Path(store.getOperatorpath() + "/" + lastFile))) {
          logger.debug("last file not moved");
          hdfsOp.rotateCall(hdfsOp.lastFile);
        }
      }
      catch (IOException ex) {
        DTThrowable.rethrow(ex);
      }
      countEmptyWindow = 0;
    }
       hdfsOp.endWindow();
  }

  public void processHiveFile(String fileMoved)
  {
    logger.info("processing {} file", fileMoved);
    String hivePartition = null;
    if (isHivePartitioned) {
      hivePartition = mapFilePartition.get(fileMoved);
      // User removes/drops a partition.
     /* if(!hivePartitions.contains(hivePartition))
        hivePartition = hivePartitions.get(0);*/
    }
    String command = getInsertCommand(store.getOperatorpath() + fileMoved, hivePartition);
    Statement stmt;
    try {
      stmt = store.getConnection().createStatement();
      //Either throw exception or log error.
      boolean result = stmt.execute(command);
      if (!result) {
        logger.error("Moving file into hive failed");
      }
    }
    catch (SQLException ex) {
      logger.error("Moving file into hive failed" + ex.getMessage());
      throw new RuntimeException("Moving file into hive failed" + ex);
    }
  }

  public String getHiveTuple(T tuple)
  {
    return tuple.toString() + "\n";
  }

  /*
   * To be implemented by the user
   */
  protected abstract String getHivePartition(T tuple);

  /*
   * To be implemented by the user
   */
  protected abstract String getInsertCommand(String filepath, String partition);

}
