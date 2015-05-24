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
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterChar;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterShort;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.sql.Date;

/*
 * An Implementation of AbstractFSRollingOutputOperator which takes any POJO as input, serializes the POJO as Hive delimiter separated values.
 * which are written in text files to hdfs. This operator can handle outputting to multiple files when the output file depends on the tuple.
 * @displayName: FSPojoToHiveOperator
 */
public class FSPojoToHiveOperator extends AbstractFSRollingOutputOperator<Object>
{
  private ArrayList<String> hivePartitionColumns;
  private ArrayList<String> hiveColumns;
  private ArrayList<FIELD_TYPE> hiveColumnsDataTypes;
  private transient ArrayList<Object> getters;
  private ArrayList<String> expressions;

  /*
   * A list of Java expressions in which each expression yields the specific table column value and partition column value in hive table from the input POJO.
   */
  public ArrayList<String> getExpressions()
  {
    return expressions;
  }

  public void setExpression(ArrayList<String> expressions)
  {
    this.expressions = expressions;
  }

  public FSPojoToHiveOperator()
  {
    super();
    getters = new ArrayList<Object>();
  }

  @SuppressWarnings("unchecked")
  private Object getValue(Object tuple, int index, FIELD_TYPE type)
  {
    Object value = null;
    switch (type) {
      case CHARACTER:
        value = ((GetterChar<Object>)getters.get(index)).get(tuple);
        break;
      case STRING:
        value = ((Getter<Object, String>) getters.get(index)).get(tuple);
        break;
      case BOOLEAN:
        value = ((GetterBoolean<Object>)getters.get(index)).get(tuple);
        break;
      case SHORT:
        value = ((GetterShort<Object>)getters.get(index)).get(tuple);
        break;
      case INTEGER:
        value = ((GetterInt<Object>)getters.get(index)).get(tuple);
        break;
      case LONG:
        value = ((GetterLong<Object>)getters.get(index)).get(tuple);
        break;
      case FLOAT:
        value = ((GetterFloat<Object>)getters.get(index)).get(tuple);
        break;
      case DOUBLE:
        value = ((GetterDouble<Object>)getters.get(index)).get(tuple);
        break;
      case DATE:
        value = ((Getter<Object,Date>)getters.get(index)).get(tuple);
        break;
      case TIMESTAMP:
        value = ((Getter<Object,Timestamp>)getters.get(index)).get(tuple);
        break;
      case OTHER:
        value = ((Getter<Object,Object>)getters.get(index)).get(tuple);
        break;
    }
    return value;
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
    Object partitionColumnValue;
    for (int i = sizeOfColumns; i < size; i++) {
      FIELD_TYPE type = hiveColumnsDataTypes.get(i);
      partitionColumnValue = getValue(tuple, sizeOfColumns, type);
      hivePartitionColumnValues.add(partitionColumnValue.toString());
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

  @SuppressWarnings("unchecked")
  public void processFirstTuple(Object tuple)
  {
    Class<?> fqcn = tuple.getClass();
    int size = hiveColumns.size() + hivePartitionColumns.size();
    for (int i = 0; i < size; i++) {
      FIELD_TYPE type = hiveColumnsDataTypes.get(i);
      final Object getter;
      final String getterExpression = expressions.get(i);
      switch (type) {
        case CHARACTER:
          getter = PojoUtils.createGetterChar(fqcn, getterExpression);
          break;
        case STRING:
          getter = PojoUtils.createGetter(fqcn, getterExpression, String.class);
          break;
        case BOOLEAN:
          getter = PojoUtils.createGetterBoolean(fqcn, getterExpression);
          break;
        case SHORT:
          getter = PojoUtils.createGetterShort(fqcn, getterExpression);
          break;
        case INTEGER:
          getter = PojoUtils.createGetterInt(fqcn, getterExpression);
          break;
        case LONG:
          getter = PojoUtils.createGetterLong(fqcn, getterExpression);
          break;
        case FLOAT:
          getter = PojoUtils.createGetterFloat(fqcn, getterExpression);
          break;
        case DOUBLE:
          getter = PojoUtils.createGetterDouble(fqcn, getterExpression);
          break;
        case DATE:
          getter = PojoUtils.createGetter(fqcn, getterExpression, Date.class);
          break;
        case TIMESTAMP:
          getter = PojoUtils.createGetter(fqcn, getterExpression, Timestamp.class);
          break;
        default:
          getter = PojoUtils.createGetter(fqcn, getterExpression, Object.class);
      }

      getters.add(getter);

    }

  }

  @Override
  protected byte[] getBytesForTuple(Object tuple)
  {
    int size = hiveColumns.size();
    StringBuilder result = new StringBuilder();
    Object value;
    for (int i = 0; i < size; i++) {
      FIELD_TYPE type = hiveColumnsDataTypes.get(i);
      value = getValue(tuple, i, type);
      result.append(value).append("\n");
    }
    return (result.toString()).getBytes();
  }

}
