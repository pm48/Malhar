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

import java.util.ArrayList;


/**
 * Hive Insert Operator which implements inserting data of primitive data type into Hive tables which
 * have column of primitive data type from files written in hdfs.
 * @param <T>
 */
public class HiveInsertOperator<T> extends AbstractHiveHDFS<T>
{
  private ArrayList<String> hivePartitions = new ArrayList<String>();

  public HiveInsertOperator()
  {
    this.store = new HiveStore();
  }

  public void addPartition(String partition)
  {
    hivePartitions.add("dt='2008-04-08'");
    hivePartitions.add("dt='2008-04-09'");
    hivePartitions.add("dt='2008-04-10'");
    hivePartitions.add("dt='2008-04-11'");
  }


  @Override
  public String getHivePartition(T tuple)
  {
    if(hivePartitions.isEmpty()){
    isPartitioned = false;
    return null;
    }
    int index = tuple.hashCode()%(hivePartitions.size()) ;
    return hivePartitions.get(index);
  }
}
