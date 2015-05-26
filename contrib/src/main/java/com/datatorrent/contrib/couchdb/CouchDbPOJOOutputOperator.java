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
package com.datatorrent.contrib.couchdb;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

public class CouchDbPOJOOutputOperator extends AbstractCouchDBOutputOperator<Object>
{
  private Getter<Object, String> getterString;
  private String getterExpr;

  public String getGetterExpr()
  {
    return getterExpr;
  }

  public void setGetterExpr(String getterExpr)
  {
    this.getterExpr = getterExpr;
  }

  @Override
  public String getDocumentId(Object tuple)
  {
    getterString = PojoUtils.createGetter(tuple.getClass(), getterExpr, String.class);
    String docId = getterString.get(tuple);
    return docId;
  }

}
