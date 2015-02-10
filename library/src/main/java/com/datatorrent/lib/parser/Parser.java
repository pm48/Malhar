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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * @param <INPUT> This is the input tuple type.
 */
public class Parser<INPUT> extends BaseOperator
{
  // List of key value pairs which has name of the field as key , data type of the field as value.
  protected ArrayList<Field> listKeyValue;


  protected Map<String,Object> outputMap;
  protected String delimiter;

  /*
   * The output is a map with key being the field name and value being the value of the field.
   */
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

  public void processTuple(INPUT tuple)
  {
    if(tuple.toString().contains(delimiter))
    {
       String[] splitInput = tuple.toString().split(delimiter);

    }
    else
      logger.debug("Delimiter not present");
  }

 public enum INPUT_TYPE
  {
    DOUBLE, INTEGER, FLOAT, LONG, SHORT, STRING, UNKNOWN
  };
  @NotNull
  INPUT_TYPE type = INPUT_TYPE.STRING;

  /**
   * This call ensures that type enum is set at setup time.
   * @param ctype the type to set the operator to.
   */
  public void setType(Class<INPUT> ctype)
  {
    if (ctype == Double.class) {
      type = INPUT_TYPE.DOUBLE;
    }
    else if (ctype == Integer.class) {
      type = INPUT_TYPE.INTEGER;
    }
    else if (ctype == Float.class) {
      type = INPUT_TYPE.FLOAT;
    }
    else if (ctype == Long.class) {
      type = INPUT_TYPE.LONG;
    }
    else if (ctype == Short.class) {
      type = INPUT_TYPE.SHORT;
    }
    else if (ctype == String.class) {
      type = INPUT_TYPE.STRING;
    }
    else {
      type = INPUT_TYPE.UNKNOWN;
    }
  }

  private class Field {
  String name;
  INPUT_TYPE type;
}
  public String getDelimiter()
  {
    return delimiter;
  }

  public void setDelimiter(String delimiter)
  {
    this.delimiter = delimiter;
  }

   public ArrayList<Field> getListKeyValue()
  {
    return listKeyValue;
  }

  public void setListKeyValue(ArrayList<Field> listKeyValue)
  {
    this.listKeyValue = listKeyValue;
  }
  
  private static final Logger logger = LoggerFactory.getLogger(Parser.class);

}
