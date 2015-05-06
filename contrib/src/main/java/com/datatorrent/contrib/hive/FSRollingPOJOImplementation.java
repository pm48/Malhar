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
 * An Implementation of AbstractFSRollingOutputOperator which takes any POJO as input.
 * @displayName: FSRollingPOJOImplementation
 */
public  class FSRollingPOJOImplementation extends AbstractFSRollingOutputOperator<Object>
{
  private ArrayList<String> hivePartitions;
 
  public ArrayList<String> getHivePartitions()
  {
    return hivePartitions;
  }

  public void setHivePartitions(ArrayList<String> hivePartitions)
  {
    this.hivePartitions = hivePartitions;
  }

  @Override
  public ArrayList<String> getHivePartition(Object tuple)
  {
    return hivePartitions;
  }

  @Override
  protected byte[] getBytesForTuple(Object tuple)
  {
    return (tuple + "\n").getBytes();
  }

}