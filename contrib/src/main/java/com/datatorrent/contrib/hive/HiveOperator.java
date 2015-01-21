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
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.google.common.collect.Lists;
import java.util.Collection;

import javax.validation.constraints.Min;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.*;
import org.apache.commons.io.output.ByteArrayOutputStream;

/*
 * Hive operator which can insert data in txt format in tables/partitions from a file written in hdfs location.
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public class HiveOperator extends AbstractStoreOutputOperator<String, HiveStore> implements Partitioner<HiveOperator>
{
  protected boolean isHivePartitioned = true;
  @Min(1)
  protected int numPartitions = 2;
  //This Property is user configurable.
  protected ArrayList<String> hivePartitionColumns = new ArrayList<String>();

  public ArrayList<String> getHivePartitionColumns()
  {
    return hivePartitionColumns;
  }

  public void setHivePartitionColumns(ArrayList<String> hivePartitionColumns)
  {
    this.hivePartitionColumns = hivePartitionColumns;
    this.isHivePartitioned = true;
  }

  protected String partition;
  @Nonnull
  protected String tablename;

  // public HDFSRollingOutputOperator<T> hdfsOp;
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
  public Collection<Partition<HiveOperator>> definePartitions(Collection<Partition<HiveOperator>> partitions, int incrementalCapacity)
  {
    int totalCount = numPartitions;
    Collection<Partition<HiveOperator>> newPartitions = Lists.newArrayListWithExpectedSize(totalCount);
    Kryo kryo = new Kryo();
    for (int i = 0; i < totalCount; i++) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      kryo.writeObject(output, this);
      output.close();
      Input lInput = new Input(bos.toByteArray());
      @SuppressWarnings("unchecked")
      HiveOperator oper = kryo.readObject(lInput, this.getClass());
      newPartitions.add(new DefaultPartition<HiveOperator>(oper));
    }
    // assign the partition keys
    DefaultPartition.assignPartitionKeys(newPartitions, input);

    return newPartitions;

  }

  @Override
  public void partitioned(Map<Integer, Partition<HiveOperator>> partitions)
  {
  }

  private static final Logger logger = LoggerFactory.getLogger(HiveOperator.class);
  private transient String appId;
  private transient int operatorId;
  //protected HashMap<String, Long> mapFilenames;

  public long getMaxWindowsWithNoData()
  {
    return maxWindowsWithNoData;
  }

  public void setMaxWindowsWithNoData(long maxWindowsWithNoData)
  {
    this.maxWindowsWithNoData = maxWindowsWithNoData;
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
   *
   * @param tuple incoming tuple
   */
  @Override
  public void processTuple(String tuple)
  {
    if (isHivePartitioned) {
      HivePartition hivePartition = new HivePartition();
      partition = hivePartition.getHivePartition(tuple);
      partition = getHivePartitionColumns().get(0) + "='" + partition + "'";
    }
    logger.info("file string path is" + tuple);
    processHiveFile(tuple);
  }

  @Override
  public void setup(OperatorContext context)
  {
    appId = context.getValue(DAG.APPLICATION_ID);
    operatorId = context.getId();
    store.setOperatorpath(store.filepath + "/" + appId + "/" + operatorId);
    super.setup(context);
  }

  @Override
  public void teardown()
  {
    super.teardown();
  }

  public void processHiveFile(String fileMoved)
  {
    logger.info("processing {} file", fileMoved);
    String command = getInsertCommand(store.getOperatorpath() + fileMoved);
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
      throw new RuntimeException("Moving file into hive failed" + ex);
    }
  }

  /*
   * User can specify multiple partitions here, giving a default implementation for one partition column here.
   */
  protected String getInsertCommand(String filepath)
  {
    String command;
    if (isHivePartitioned) {
    //  if (!hdfsOp.isHDFSLocation()) {
      //    command = "load data local inpath '" + filepath + "' OVERWRITE into table " + tablename + " PARTITION " + "( " + partition + " )";
      //  }
      //  else {
      command = "load data local inpath '" + filepath + "' OVERWRITE into table " + tablename + " PARTITION " + "( " + partition + " )";
      //  }
    }
    else {
     // if (!hdfsOp.isHDFSLocation()) {
      //   command = "load data local inpath '" + filepath + "' OVERWRITE into table " + tablename;
      // }
      // else {
      command = "load data local inpath '" + filepath + "' OVERWRITE into table " + tablename;
      // }
    }
    logger.info("command is {}", command);
    return command;

  }



}
