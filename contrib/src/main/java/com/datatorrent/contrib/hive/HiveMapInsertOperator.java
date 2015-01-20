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

import java.util.Map;
import javax.validation.constraints.NotNull;



/**
 * Hive Insert Map Operator which implements inserting data of type map into Hive table partitions
 * having column of map data type from files written in HDFS.
 * @param <T>
 */
public class HiveMapInsertOperator<T  extends Map<?,?>> extends AbstractHiveHDFS<T>
{
  @NotNull
  public String delimiter;

  public String getDelimiter()
  {
    return delimiter;
  }

  public void setDelimiter(String delimiter)
  {
    this.delimiter = delimiter;
  }

  public HiveMapInsertOperator(){
    this.store = new HiveStore();
  }

 /* @Override
  @SuppressWarnings("rawtypes")
  public String getHiveTuple(T tuple)
  {
    Iterator keyIter = tuple.keySet().iterator();
    StringBuilder writeToHive = new StringBuilder("");

    while(keyIter.hasNext()){
     Object key = keyIter.next();
     Object obj = tuple.get(key);
     writeToHive.append(key).append(delimiter).append(obj).append("\n");
    }
    return writeToHive.toString();
  }*/

  @Override
  public String getHivePartition(T tuple)
  {
    return tuple.toString();
  }



  /*
   * User can specify multiple partitions here, giving a default implementation for one partition column here.
   */
  @Override
  protected String getInsertCommand(String filepath, String partition)
  {
    String command;
    if (isHivePartitioned) {
     // if (!hdfsOp.isHDFSLocation()) {
     //   command = "load data local inpath '" + filepath + "' OVERWRITE into table " + tablename + " PARTITION " + "( " + partition + " )";
     // }
     // else {
        command = "load data inpath '" + filepath + "' OVERWRITE into table " + tablename + " PARTITION " + "( " + partition + " )";
     // }
    }
    else {
     // if (!hdfsOp.isHDFSLocation()) {
     //   command = "load data local inpath '" + filepath + "' OVERWRITE into table " + tablename;
     // }
     // else {
        command = "load data inpath '" + filepath + "' OVERWRITE into table " + tablename;
     // }
    }

    return command;

  }


}
