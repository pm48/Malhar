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
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterChar;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterObject;
import com.datatorrent.lib.util.PojoUtils.GetterShort;
import com.datatorrent.lib.util.PojoUtils.GetterString;
import java.util.ArrayList;

/*
 * An Implementation of AbstractFSRollingOutputOperator which takes any POJO as input.
 * @displayName: FSRollingPOJOImplementation
 */
public  class FSRollingPOJOImplementation extends AbstractFSRollingOutputOperator<Object>
{
  private ArrayList<String> hivePartitionColumns;
  private ArrayList<FIELD_TYPE> hivePartitionColumnsDataTypes;

  private ArrayList<String> expression;
  private transient ArrayList<Object> getters;
  private transient boolean isFirstTuple;

  public FSRollingPOJOImplementation()
  {
    super();
    isFirstTuple = true;
    getters = new ArrayList<Object>();
  }

  public enum FIELD_TYPE
  {
    BOOLEAN, DOUBLE, INTEGER, FLOAT, LONG, SHORT, CHARACTER, STRING, DATE, TIMESTAMP, OTHER
  };

  /*
   * A Java expression that will yield the specific partition column in hive table from the input POJO.
   */
  public ArrayList<String> getExpression()
  {
    return expression;
  }

  public void setExpression(ArrayList<String> expression)
  {
    this.expression = expression;
  }

  /*
   * Partition Columns in Hive table.Example: dt for date,ts for timestamp
   */
  public ArrayList<String> getHivePartitionColumns()
  {
    return hivePartitionColumns;
  }

  public void setHivePartitionColumnsValues(ArrayList<String> hivePartitionColumns)
  {
    this.hivePartitionColumns = hivePartitionColumns;
  }

  /*
   * Data Types of Partition Columns in Hive table.Example: date for dt column,timestamp for ts column.
   * Particular Data Type can be chosen from the List of data types provided.
   */
  public ArrayList<FIELD_TYPE> getHivePartitionColumnsDataTypes()
  {
    return hivePartitionColumnsDataTypes;
  }

  public void setHivePartitionColumnsDataTypes(ArrayList<FIELD_TYPE> hivePartitionColumnsDataTypes)
  {
    this.hivePartitionColumnsDataTypes = hivePartitionColumnsDataTypes;
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
  public void processTuple(Object tuple)
  {
    if (isFirstTuple) {
      processFirstTuple(tuple);
    }
    isFirstTuple = false;
    super.processTuple(tuple);
  }

  public void processFirstTuple(Object tuple)
  {
    Class<?> fqcn = tuple.getClass();
    int size = hivePartitionColumns.size();
    for (int i = 0; i < size; i++) {
      FIELD_TYPE type = hivePartitionColumnsDataTypes.get(i);
      String getterExpression = expression.get(i);
      if (type == FIELD_TYPE.CHARACTER) {
        GetterChar getChar = PojoUtils.createGetterChar(fqcn, getterExpression);
        getters.add(getChar);
      }
      else if (type == FIELD_TYPE.STRING) {
        GetterString getString = PojoUtils.createGetterString(fqcn, getterExpression);
        getters.add(getString);
      }
      else if (type == FIELD_TYPE.BOOLEAN) {
        GetterBoolean getBoolean = PojoUtils.createGetterBoolean(fqcn, getterExpression);
        getters.add(getBoolean);
      }
      else if (type == FIELD_TYPE.SHORT) {
        GetterShort getShort = PojoUtils.createGetterShort(fqcn, getterExpression);
        getters.add(getShort);
      }
      else if (type == FIELD_TYPE.INTEGER) {
        GetterInt getInt = PojoUtils.createGetterInt(fqcn, getterExpression);
        getters.add(getInt);
      }
      else if (type == FIELD_TYPE.LONG) {
        GetterLong getLong = PojoUtils.createExpressionGetterLong(fqcn, getterExpression);
        getters.add(getLong);
      }
      else if (type == FIELD_TYPE.FLOAT) {
        GetterFloat getFloat = PojoUtils.createGetterFloat(fqcn, getterExpression);
        getters.add(getFloat);
      }
      else if (type == FIELD_TYPE.DOUBLE) {
        GetterDouble getDouble = PojoUtils.createGetterDouble(fqcn, getterExpression);
        getters.add(getDouble);
      }
      else if (type == FIELD_TYPE.DATE) {
        GetterObject getObject = PojoUtils.createGetterObject(fqcn, getterExpression);
        getters.add(getObject);
      }
      else if (type == FIELD_TYPE.TIMESTAMP) {
        GetterObject getObject = PojoUtils.createGetterObject(fqcn, getterExpression);
        getters.add(getObject);
      }
      else if (type == FIELD_TYPE.OTHER) {
        GetterObject getObject = PojoUtils.createGetterObject(fqcn, getterExpression);
        getters.add(getObject);
      }

    }
  }



  @Override
  protected byte[] getBytesForTuple(Object tuple)
  {
    //return (tuple + "\n").getBytes();
    return (getters.get(tuple).toString() + "\n").getBytes();
  }

}