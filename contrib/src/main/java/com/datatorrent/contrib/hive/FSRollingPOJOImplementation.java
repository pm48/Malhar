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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.sql.Date;

/*
 * An Implementation of AbstractFSRollingOutputOperator which takes any POJO as input.
 * @displayName: FSRollingPOJOImplementation
 */
public class FSRollingPOJOImplementation extends AbstractFSRollingOutputOperator<Object>
{
  private ArrayList<String> hivePartitionColumns;
  private ArrayList<String> hiveColumns;
  private ArrayList<FIELD_TYPE> hiveColumnsDataTypes;
  private transient ArrayList<Object> getters;
  private ArrayList<String> expression;

  /*
   * A list of Java expressions in which each expression yields the specific table column value and partition column value in hive table from the input POJO.
   */
  public ArrayList<String> getExpression()
  {
    return expression;
  }

  public void setExpression(ArrayList<String> expression)
  {
    this.expression = expression;
  }

  public FSRollingPOJOImplementation()
  {
    super();
    getters = new ArrayList<Object>();
  }

  private Object getGetter(Object tuple, int index, FIELD_TYPE type)
  {
    Object getter;
    switch (type) {
      case CHARACTER:
        getter = ((GetterChar)getters.get(index)).get(tuple);
        break;
      case STRING:
        getter = ((GetterString)getters.get(index)).get(tuple);
        break;
      case BOOLEAN:
        getter = ((GetterBoolean)getters.get(index)).get(tuple);
        break;
      case SHORT:
        getter = ((GetterShort)getters.get(index)).get(tuple);
        break;
      case INTEGER:
        getter = ((GetterInt)getters.get(index)).get(tuple);
        break;
      case LONG:
        getter = ((GetterLong)getters.get(index)).get(tuple);
        break;
      case FLOAT:
        getter = ((GetterFloat)getters.get(index)).get(tuple);
        break;
      case DOUBLE:
        getter = ((GetterDouble)getters.get(index)).get(tuple);
        break;
      case DATE:
        getter = (Date)((GetterObject)getters.get(index)).get(tuple);
        break;
      case TIMESTAMP:
        getter = (Timestamp)((GetterObject)getters.get(index)).get(tuple);
        break;
      case OTHER:
        getter = (Timestamp)((GetterObject)getters.get(index)).get(tuple);
        break;
      default:
        getter = ((GetterObject)getters.get(index)).get(tuple);
        break;
    }
    return getter;
  }

  public enum FIELD_TYPE
  {
    BOOLEAN, DOUBLE, INTEGER, FLOAT, LONG, SHORT, CHARACTER, STRING, DATE, TIMESTAMP, OTHER
  };

  /*
   * Columns in Hive table.
   */
  public ArrayList<String> getHiveColumns()
  {
    return hiveColumns;
  }

  public void setHiveColumns(ArrayList<String> hiveColumns)
  {
    this.hiveColumns = hiveColumns;
  }

  /*
   * Partition Columns in Hive table.Example: dt for date,ts for timestamp
   */
  public ArrayList<String> getHivePartitionColumns()
  {
    return hivePartitionColumns;
  }

  public void setHivePartitionColumns(ArrayList<String> hivePartitionColumns)
  {
    this.hivePartitionColumns = hivePartitionColumns;
  }

  /*
   * Data Types of Hive table data columns and Partition Columns.
   * Example: If the Hive table has two columns of data type int and float and is partitioned by date of type String,
   * then hiveColumnsDataTypes = {INTEGER,STRING,STRING}.
   * Particular Data Type can be chosen from the List of data types provided.
   */
  public ArrayList<FIELD_TYPE> getHiveColumnsDataTypes()
  {
    return hiveColumnsDataTypes;
  }

  public void setHiveColumnsDataTypes(ArrayList<FIELD_TYPE> hiveColumnsDataTypes)
  {
    this.hiveColumnsDataTypes = hiveColumnsDataTypes;
  }

  @Override
  public ArrayList<String> getHivePartition(Object tuple)
  {
    int sizeOfColumns = hiveColumns.size();
    int sizeOfPartitionColumns = hivePartitionColumns.size();
    int size = sizeOfColumns + sizeOfPartitionColumns;
    ArrayList<String> hivePartitionColumnValues = new ArrayList<String>();
    Object getter;
    for (int i = sizeOfColumns; i < size; i++) {
      FIELD_TYPE type = hiveColumnsDataTypes.get(i);
      getter = getGetter(tuple, sizeOfColumns, type);
      hivePartitionColumnValues.add(getter.toString());
    }
    return hivePartitionColumnValues;
  }

  @Override
  public void processTuple(Object tuple)
  {
    if (getters.isEmpty()) {
      processFirstTuple(tuple);
    }
    super.processTuple(tuple);
  }

  public void processFirstTuple(Object tuple)
  {
    Class<?> fqcn = tuple.getClass();
    int size = hiveColumns.size() + hivePartitionColumns.size();
    for (int i = 0; i < size; i++) {
      FIELD_TYPE type = hiveColumnsDataTypes.get(i);
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
    int size = hiveColumns.size();
    StringBuilder result = new StringBuilder();
    Object getter;
    for (int i = 0; i < size; i++) {
      FIELD_TYPE type = hiveColumnsDataTypes.get(i);
      getter = getGetter(tuple, i, type);
      result.append(getter).append("\n");
    }
    return (result.toString()).getBytes();
  }

}
