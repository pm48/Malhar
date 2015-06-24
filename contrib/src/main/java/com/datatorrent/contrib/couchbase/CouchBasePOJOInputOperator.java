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
import java.util.Iterator;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.ektorp.ViewQuery;

public class CouchBasePOJOInputOperator extends AbstractCouchBaseInputOperator<Object>
{
  private String objectClass;
  //Value stored in Couchbase can be of these data types: boolean,numeric,string,arrays,object,null.
  private transient Setter<Object, Object> setter;
  private Class<?> className;
  private String mapFunctionQuery;
  private String startkey;
  @Min(1)
  private int limit = 10;
  private String startDocId;
  @NotNull
  //private Query query;
  private String designDocumentName;
  private transient ArrayList<String> keys;
  private String expressionForValue;

  @Override
  public ArrayList<String> getKeys()
  {
    return keys;
  }

  public void setKeys(ArrayList<String> keys)
  {
    this.keys = keys;
  }

  public String getStartDocId()
  {
    return startDocId;
  }

  public void setStartDocId(String startDocId)
  {
    this.startDocId = startDocId;
  }

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


  public String getMapFunctionQuery()
  {
    return mapFunctionQuery;
  }

  public void setMapFunctionQuery(String mapFunctionQuery)
  {
    this.mapFunctionQuery = mapFunctionQuery;
  }

  public String getObjectClass()
  {
    return objectClass;
  }

  public void setObjectClass(String objectClass)
  {
    this.objectClass = objectClass;
  }


  @Override
  public void emitTuples()
  {
    boolean hasRow = true;
    Query query = new Query();
    query.setStale( Stale.FALSE );
    query.setLimit(limit);

   while(hasRow){
     hasRow = false;
query.setRangeStart(startkey);
   View view = store.getInstance().getView(designDocumentName, viewName);

   ViewResponse result =store.getInstance().query(view, query);
Iterator<ViewRow> iterRow =  result.iterator();
 while(iterRow.hasNext())
    {
      hasRow = true;
      System.out.println("row key and " + iterRow.next().getKey());
      startkey = iterRow.next().getKey();
      startDocId = iterRow.next().getId();
      Object result1 = iterRow.next().getDocument();
      keys.add(startkey);

         if (result1 != null) {
        Object tuple = getTuple(result);
        outputPort.emit(tuple);
      }

    }

   System.out.println("size of result is "+result.size());


    //for (ViewRow row : result) {
    //System.out.println(row);
//}
    /*Iterator<ViewRow> iterRow =  response.iterator();
    keys = new ArrayList<String>();
    while(iterRow.hasNext())
    {
      hasRow = true;
      System.out.println("row key and docid are " + iterRow.next().getKey() + iterRow.next().getId());
      startkey = iterRow.next().getKey();
      startDocId = iterRow.next().getId();
      Object result = iterRow.next().getDocument();
      keys.add(startkey);

         if (result != null) {
        Object tuple = getTuple(result);
        outputPort.emit(tuple);
      }

    }*/

  }
  }


  @Override
  public Object getTuple(Object couchbaseObject)
  {
    if (setter == null) {
      try {
        className = Class.forName(objectClass);
      }
      catch (ClassNotFoundException ex) {
        throw new RuntimeException(ex);
      }
      //for(int i=0;i<size;i++){
      setter = PojoUtils.createSetter(className, expressionForValue, Object.class);
      //}
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
    setter.set(outputObj, couchbaseObject);

    return outputObj;
  }



}
