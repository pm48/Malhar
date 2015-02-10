/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.parser;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyValPair;
import java.util.ArrayList;
import java.util.Map;
import javax.validation.constraints.NotNull;

/*
 * @param <INPUT> This is the input tuple type.
 */
public class Parser<INPUT> extends BaseOperator
{
  // List of key value pairs which has name of the field , data type of the field.
  protected ArrayList<KeyValPair<String,Object>> listKeyValue;
  protected Map<String,Object> outputMap;
  protected String delimiter;

  public final transient DefaultOutputPort<Map<String,Object>> data = new DefaultOutputPort<Map<String,Object>>();

  /**
   * This input port receives incoming tuples.
   */
  public final transient DefaultInputPort<INPUT> input = new DefaultInputPort<INPUT>()
  {
    @Override
    public void process(INPUT tuple)
    {
      processTuple(tuple);
    }
  };

 public enum V_TYPE
  {
    DOUBLE, INTEGER, FLOAT, LONG, SHORT, STRING, UNKNOWN
  };
  @NotNull
  V_TYPE type = V_TYPE.STRING;

  /**
   * This call ensures that type enum is set at setup time.
   * @param ctype the type to set the operator to.
   */
  public void setType(Class<INPUT> ctype)
  {
    if (ctype == Double.class) {
      type = V_TYPE.DOUBLE;
    }
    else if (ctype == Integer.class) {
      type = V_TYPE.INTEGER;
    }
    else if (ctype == Float.class) {
      type = V_TYPE.FLOAT;
    }
    else if (ctype == Long.class) {
      type = V_TYPE.LONG;
    }
    else if (ctype == Short.class) {
      type = V_TYPE.SHORT;
    }
    else {
      type = V_TYPE.UNKNOWN;
    }
  }

  public String getDelimiter()
  {
    return delimiter;
  }

  public void setDelimiter(String delimiter)
  {
    this.delimiter = delimiter;
  }

  public ArrayList<KeyValPair<String, Object>> getListKeyValue()
  {
    return listKeyValue;
  }

  public void setListKeyValue(ArrayList<KeyValPair<String, Object>> listKeyValue)
  {
    this.listKeyValue = listKeyValue;
  }


}
