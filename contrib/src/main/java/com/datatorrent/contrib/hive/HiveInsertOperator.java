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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Hive Insert Operator which implements inserting data of primitive data type into Hive table partitions
 * which have column of primitive data type from files written in HDFS.
 * @param <T>
 */
public class HiveInsertOperator<T> extends AbstractHiveHDFS<T>
{
  private static final Logger logger = LoggerFactory.getLogger(HiveInsertOperator.class);
  public HiveInsertOperator()
  {
    this.store = new HiveStore();
  }

  @Override
  public String getHivePartition(T tuple)
  {
    if(hivePartitions.isEmpty()){
    isHivePartitioned = false;
    return null;
    }
    int index = tuple.hashCode() %(hivePartitions.size()) ;
    return hivePartitions.get(index);
  }

  /*
   * User can specify multiple partitions here, giving a default implementation for one partition column here.
   */
  @Override
  protected String getInsertCommand(String filepath, String partition)
  {
    String command;
    if (isHivePartitioned) {
      if (!hdfsOp.isHDFSLocation()) {
        command = "load data local inpath '" + filepath + "' OVERWRITE into table " + tablename + " PARTITION " + "( " + partition + " )";
      }
      else {
        command = "load data inpath '" + filepath + "' OVERWRITE into table " + tablename + " PARTITION " + "( " + partition + " )";
      }
      if (!hivePartitions.contains(partition)) {
        logger.info("new partition is {}" , partition);
        hivePartitions.add(partition);
      }
    }
    else {
      if (!hdfsOp.isHDFSLocation()) {
        command = "load data local inpath '" + filepath + "' OVERWRITE into table " + tablename;
      }
      else {
        command = "load data inpath '" + filepath + "' OVERWRITE into table " + tablename;
      }
    }

    return command;

  }

  @Override
  public void addPartition(String partition)
  {
    hivePartitions.add("dt='2014-12-18'");
    hivePartitions.add(partition);
  }

  @Override
  protected void dropPartition(String partition)
  {
    hivePartitions.remove(partition);
  }

}
