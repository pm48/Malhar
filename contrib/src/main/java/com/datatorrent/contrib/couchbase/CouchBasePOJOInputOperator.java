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
package com.datatorrent.contrib.couchbase;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Setter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CouchBasePOJOInputOperator extends AbstractCouchBaseInputOperator<Object>
{
  private String objectClass;
  //Value stored in Couchbase can be of these data types: boolean,numeric,string,arrays,object,null.
  private final transient ArrayList<Setter<Object, Object>> valueSetters;
  private Class<?> className;

  private ArrayList<String> expressionForValue;

  public ArrayList<String> getExpressionForValue()
  {
    return expressionForValue;
  }

  public void setExpressionForValue(ArrayList<String> expressionForValue)
  {
    this.expressionForValue = expressionForValue;
  }


  private transient ArrayList<String> keys;

  public void setKeys(ArrayList<String> keys)
  {
    this.keys = keys;
  }


  public String getObjectClass()
  {
    return objectClass;
  }

  public void setObjectClass(String objectClass)
  {
    this.objectClass = objectClass;
  }

  public CouchBasePOJOInputOperator()
  {
    super();
    valueSetters = new ArrayList<Setter<Object, Object>>();
  }

  @Override
  public Object getTuple(Object couchbaseObject)
  {
    int size = expressionForValue.size();
    if (valueSetters.isEmpty()) {
      try {
        className = Class.forName(objectClass);
      }
      catch (ClassNotFoundException ex) {
        throw new RuntimeException(ex);
      }
      for(int i=0;i<size;i++){
      valueSetters.add(PojoUtils.createSetter(className, expressionForValue.get(i), Object.class));
      }
    }
    Object outputObj = null;
    try {
      outputObj = className.newInstance();
    }
    catch (InstantiationException ex) {
      throw new RuntimeException(ex);
    }
    catch (IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }
    for(int i=0;i<size;i++){
    valueSetters.get(i).set(outputObj, couchbaseObject);
    }
    return outputObj;
  }

  @Override
  public List<String> getKeys()
  {
    return keys;
  }

}
