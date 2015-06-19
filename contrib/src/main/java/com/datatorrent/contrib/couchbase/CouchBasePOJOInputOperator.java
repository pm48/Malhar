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

import com.couchbase.client.protocol.views.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Setter;
import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.Min;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;

public class CouchBasePOJOInputOperator extends AbstractCouchBaseInputOperator<Object>
{
  private String objectClass;
  //Value stored in Couchbase can be of these data types: boolean,numeric,string,arrays,object,null.
  private final transient ArrayList<Setter<Object, Object>> valueSetters;
  private Class<?> className;
  private String mapFunctionQuery;
  private String startkey;
  @Min(1)
  private int limit = 10;
  @Min(1)
  private int skip=1;
  private String designDocumentName;

  public String getDesignDocumentName()
  {
    return designDocumentName;
  }

  public void setDesignDocumentName(String designDocumentName)
  {
    this.designDocumentName = designDocumentName;
  }

  public String getViewName()
  {
    return viewName;
  }

  public void setViewName(String viewName)
  {
    this.viewName = viewName;
  }
  private String viewName;

  public int getSkip()
  {
    return skip;
  }

  public void setSkip(int skip)
  {
    this.skip = skip;
  }

  public int getLimit()
  {
    return limit;
  }

  public void setLimit(int limit)
  {
    this.limit = limit;
  }

  public String getStartkey()
  {
    return startkey;
  }

  public void setStartkey(String startkey)
  {
    this.startkey = startkey;
  }


  private ArrayList<String> expressionForValue;

  public ArrayList<String> getExpressionForValue()
  {
    return expressionForValue;
  }

  public void setExpressionForValue(ArrayList<String> expressionForValue)
  {
    this.expressionForValue = expressionForValue;
  }

  public String getMapFunctionQuery()
  {
    return mapFunctionQuery;
  }

  public void setMapFunctionQuery(String mapFunctionQuery)
  {
    this.mapFunctionQuery = mapFunctionQuery;
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
  public void setup(OperatorContext context)
  {
    super.setup(context);
    DesignDocument designDoc = new DesignDocument(designDocumentName);

    ViewDesign viewDesign = new ViewDesign(viewName,getMapFunction());
    designDoc.getViews().add(viewDesign);
    store.getInstance().createDesignDoc( designDoc );
  }

  @Override
  public void emitTuples()
  {
    /*ViewResult result = bucket.query(ViewQuery.from("designdoc", "myview"));

// Iterate through the returned ViewRows
for (ViewRow row : result) {
    System.out.println(row);
}

for(ViewRow row : result) {
  row.getDocument(); // deal with the document/data
}
Object result = null;*/
    for (String key: keys) {
        int master = conf.getMaster(conf.getVbucketByKey(key));
        if (master == getServerIndex()) {
          result = clientPartition.get(key);
        }
      }

      if (result != null) {
        T tuple = getTuple(result);
        outputPort.emit(tuple);
      }
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

  @Override
  protected String getMapFunction()
  {
    return mapFunctionQuery;
  }

}
