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

import java.util.Random;


public  class HivePartition<T> implements HivePartitionInterface<T>
{
  /**
   * The random object use to generate the tuples.
   */
  private transient Random random = new Random();
  public HivePartition(){
  }


  @Override
  public  String getHivePartition(T tuple)
  {
    return "2014-12-1" + random.nextInt(100);
  }

}
