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

import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.Collection;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.datatorrent.lib.db.AbstractStoreOutputOperator;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.contrib.hive.FSRollingOutputOperator.FilePartitionMapping;

/*
 * Hive operator which can insert data in txt format in tables/partitions from a file written in hdfs location.
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public class HiveOperator extends AbstractStoreOutputOperator<FilePartitionMapping, HiveStore> implements Partitioner<HiveOperator>
{
  @Min(1)
  protected int numPartitions = 2;
  //This Property is user configurable.
  protected ArrayList<String> hivePartitionColumns = new ArrayList<String>();
  protected String partition;
  @Nonnull
  protected String tablename;
  private transient String appId;
  private transient int operatorId;
  //This variable is user configurable.
  @Min(0)
  private transient long maxWindowsWithNoData = 100;

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

  @Override
  public void setup(OperatorContext context)
  {
    appId = context.getValue(DAG.APPLICATION_ID);
    operatorId = context.getId();
    store.setOperatorpath(store.filepath + "/" + appId + "/" + operatorId);
    super.setup(context);
  }

  /**
   * Function to process each incoming tuple
   * This can be overridden by user for multiple partition columns.
   * Giving an implementation for one partition column.
   *
   * @param tuple incoming tuple which has filename and hive partition.
   */
  @Override
  public void processTuple(FilePartitionMapping tuple)
  {
    String fileMoved = tuple.getFilename();
    partition = tuple.getPartition();
    processHiveFile(fileMoved);

  }

  public void processHiveFile(String fileMoved)
  {
    logger.debug("processing {} file", fileMoved);
    String command = getInsertCommand(fileMoved);
    Statement stmt;
    try {
      stmt = store.getConnection().createStatement();
      stmt.execute(command);
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
    if (partition != null) {
      filepath = store.getOperatorpath() + "/" + partition + "/" + filepath;
      partition = getHivePartitionColumns().get(0) + "='" + partition + "'";
      command = "load data local inpath '" + filepath + "' OVERWRITE into table " + tablename + " PARTITION" + "( " + partition + " )";
    }
    else {
      filepath = store.getOperatorpath() + "/" + filepath;
      command = "load data local inpath '" + filepath + "' OVERWRITE into table " + tablename;
    }
    logger.debug("command is {}", command);
    return command;

  }

  public ArrayList<String> getHivePartitionColumns()
  {
    return hivePartitionColumns;
  }

  public void setHivePartitionColumns(ArrayList<String> hivePartitionColumns)
  {
    this.hivePartitionColumns = hivePartitionColumns;
  }

  public int getNumPartitions()
  {
    return numPartitions;
  }

  public void setNumPartitions(int numPartitions)
  {
    this.numPartitions = numPartitions;
  }

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

  private static final Logger logger = LoggerFactory.getLogger(HiveOperator.class);

}
