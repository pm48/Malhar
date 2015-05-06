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

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.GetterObject;
import java.util.ArrayList;

/*
 * An Implementation of AbstractFSRollingOutputOperator which takes any POJO as input.
 * @displayName: FSRollingPOJOImplementation
 */
public  class FSRollingPOJOImplementation extends AbstractFSRollingOutputOperator<Object>
{
  private ArrayList<String> hivePartitionColumnValues;
  private String expression;
  private GetterObject getter;
  private boolean isfirstTuple = true;

  /*
   * A Java expression that will yield the specific column in hive table from the input POJO.
   */
  public String getExpression()
  {
    return expression;
  }

  public void setExpression(String expression)
  {
    this.expression = expression;
  }

  /*
   * Values for Partition Columns in Hive table.
   * Example: If partition columns are date and country, then values can be 2015-12-12,USA
   * OR 2015-12-12,UK etc.
   */
  public ArrayList<String> getHivePartitionColumnValues()
  {
    return hivePartitionColumnValues;
  }

  public void setHivePartitionColumnsValues(ArrayList<String> hivePartitionColumnsValues)
  {
    this.hivePartitionColumnValues = hivePartitionColumnsValues;
  }

  @Override
  public ArrayList<String> getHivePartition(Object tuple)
  {
    for(int i=0;i<hivePartitionColumnValues.size();i++)
    {
     // tuple.
    }
    return hivePartitionColumnValues;
  }

  @Override
  protected void processTuple(Object tuple)
  {
    if(isfirstTuple)
    {
      Class<?> fqcn = tuple.getClass();
      getter = PojoUtils.createGetterObject(fqcn, expression);
    }
    isfirstTuple= false;
    super.processTuple(tuple);
  }



  @Override
  protected byte[] getBytesForTuple(Object tuple)
  {
    //return (tuple + "\n").getBytes();
    return (getter.get(tuple).toString() + "\n").getBytes();
  }

}