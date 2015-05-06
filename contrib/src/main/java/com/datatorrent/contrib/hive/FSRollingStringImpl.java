/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

/*
 * An Implementation of AbstractFSRollingOutputOperator which takes a String as input.
 * @displayName: FSRollingStringImplementation
 */
public  class FSRollingStringImpl extends AbstractFSRollingOutputOperator<String>
{
  private ArrayList<String> hivePartitionColumnValues;

  /*
   * Values for Partition Columns in Hive table.
   * Example: If partition columns are date and country, then values can be 2015-12-12,USA
   * OR 2015-12-12,UK etc.
   */
  public ArrayList<String> getHivePartitionColumnsValues()
  {
    return hivePartitionColumnValues;
  }

  public void setHivePartitionColumnsValues(ArrayList<String> hivePartitionColumnsValues)
  {
    this.hivePartitionColumnValues = hivePartitionColumnsValues;
  }

  @Override
  public ArrayList<String> getHivePartition(String tuple)
  {
    return hivePartitionColumnValues;
  }

  @Override
  protected byte[] getBytesForTuple(String tuple)
  {
    return (tuple + "\n").getBytes();
  }

}