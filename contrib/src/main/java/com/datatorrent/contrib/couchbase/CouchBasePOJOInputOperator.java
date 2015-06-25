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
import java.util.List;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.ektorp.ViewQuery;

public class CouchBasePOJOInputOperator extends AbstractCouchBaseInputOperator<Object>
{
  private transient List<Setter<Object, Object>> setters;
  private transient Class<?> className = null;
  private transient ArrayList<String> keys;
  private final transient List<Class<?>> fieldType;
  //User has the option to specify a start key.
  private String startkey;
  @Min(1)
  private int limit = 10;
  private String startDocId;
  @NotNull
  private String designDocumentName;
  @NotNull
  private ArrayList<String> columns;

  @NotNull
  private String viewName;
  /*
   * POJO class which is generated as output from this operator.
   * Example:
   * public class TestPOJO{ int intfield; public int getInt(){} public void setInt(){} }
   * outputClass = TestPOJO
   * POJOs will be generated on fly in later implementation.
   */
  private String outputClass;

  public String getOutputClass()
  {
    return outputClass;
  }

  public void setOutputClass(String outputClass)
  {
    this.outputClass = outputClass;
  }

  public ArrayList<String> getColumns()
  {
    return columns;
  }

  public void setColumns(ArrayList<String> columns)
  {
    this.columns = columns;
  }

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

  public CouchBasePOJOInputOperator()
  {
    fieldType = new ArrayList<Class<?>>();
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    setters = new ArrayList<Setter<Object, Object>>();
    try {
      className = Class.forName(outputClass);
    }
    catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }

    for (int i = 0; i < columns.size(); i++) {
      Class<?> type = null;
      try {
        type = className.getDeclaredField(columns.get(i)).getType();
      }
      catch (NoSuchFieldException ex) {
        throw new RuntimeException(ex);
      }
      catch (SecurityException ex) {
        throw new RuntimeException(ex);
      }
      fieldType.add(type);
      setters.add(PojoUtils.createSetter(className, columns.get(i), type));

    }
  }

  @Override
  public void emitTuples()
  {
    boolean hasRow = true;
    Query query = new Query();
    query.setStale(Stale.FALSE);
    query.setLimit(limit);
    while (hasRow) {
      hasRow = false;
      if (startkey != null) {
        query.setRangeStart(startkey);
      }
      View view = store.getInstance().getView(designDocumentName, viewName);

      ViewResponse result = store.getInstance().query(view, query);
      Iterator<ViewRow> iterRow = result.iterator();
      while (iterRow.hasNext()) {
        hasRow = true;
        System.out.println("row key is " + iterRow.next().getKey());
        System.out.println("row id is " + iterRow.next().getId());
        startkey = iterRow.next().getKey();
        startDocId = iterRow.next().getId();
        String value = iterRow.next().getValue();
        System.out.println("value is " + value);
        keys.add(startkey);
        String[] values = null;
        if (value != null) {
          values = value.split(",");
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
        for (int i = 0; i < setters.size(); i++) {
          setters.get(i).set(outputObj, values[i]);
        }

        outputPort.emit(outputObj);

      }
    }
  }

  //This method is not required in current concrete implementation of AbstractCouchBaseInputOperator.
  @Override
  public Object getTuple(Object value)
  {
    return null;
  }

}
