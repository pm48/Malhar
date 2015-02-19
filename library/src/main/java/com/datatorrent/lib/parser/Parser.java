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

import java.util.ArrayList;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.prefs.CsvPreference;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.DTThrowable;
import java.io.*;
import java.util.HashMap;
import org.supercsv.cellprocessor.*;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.*;

/*
 * @param <INPUT> This is the input tuple type.
 */
public class Parser<INPUT> extends BaseOperator
{
  // List of key value pairs which has name of the field as key , data type of the field as value.
  @Nonnull
  protected ArrayList<Field> listKeyValue;
  protected String inputEncoding;
  @Nonnull
  protected int fieldDelimiter;
  protected String lineDelimiter;
  protected transient String[] properties;
  protected transient CellProcessor[] processors;
  Map<String, Object> fieldValueMapping = new HashMap<String, Object>();
  protected ArrayList<Map<String, Object>> arrayMaps = new ArrayList<Map<String, Object>>();

  public enum INPUT_TYPE
  {
    BOOLEAN, DOUBLE, INTEGER, FLOAT, LONG, SHORT, CHARACTER, STRING, DATE, UNKNOWN
  };

  @NotNull
  INPUT_TYPE type;

  public Parser()
  {
    inputEncoding = "UTF8";
    lineDelimiter = "\r\n";
    type = INPUT_TYPE.STRING;
  }

  /**
   * The output is a map with key being the field name and value being the value of the field.
   */
  public final transient DefaultOutputPort<Map<String, Object>> output = new DefaultOutputPort<Map<String, Object>>();

  /**
   * This input port receives incoming tuples.
   */
  public final transient DefaultInputPort<INPUT> input1 = new DefaultInputPort<INPUT>()
  {
    @Override
    public void process(INPUT tuple)
    {
      ICsvMapReader csvReader = null;
      CsvPreference preference = new CsvPreference.Builder('"', fieldDelimiter, lineDelimiter).build();
      InputStream in;
      BufferedReader br = null;

      try {
        in = new ByteArrayInputStream(tuple.toString().getBytes(inputEncoding));
        br = new BufferedReader(new InputStreamReader(in, inputEncoding));
      }
      catch (UnsupportedEncodingException ex) {
        logger.error("Encoding not supported", ex);
      }
      csvReader = new CsvMapReader(br, preference);
      try {
        String[] header = csvReader.getHeader(true);
        logger.info("length of header is {}", header.length);
        int len = header.length;
        if (len > properties.length) {
          logger.debug("More column values in csv string than user supplied field properties.");
        }
        else if (len < properties.length) {
          logger.debug("Less column values in csv string than user supplied field properties.");
        }
        // else {
        for (int i = 0; i < len; i++) {
          logger.info("header is {}", header[i]);
          fieldValueMapping.put(properties[i], header[i]);
        }
        logger.debug("fieldValueMapping is {}", fieldValueMapping.toString());
        //arrayMaps.add(fieldValueMapping);
        while (true) {
          fieldValueMapping = csvReader.read(properties, processors);
          if (fieldValueMapping == null) {
            break;
          }
          logger.debug("fieldValueMapping in loop is {}", fieldValueMapping.toString());
          output.emit(fieldValueMapping);
        }
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      finally {
        if (csvReader != null) {
          try {
            csvReader.close();
          }
          catch (IOException ex) {
            DTThrowable.rethrow(ex);
          }
        }
      }

    }

  };

  /**
   * This input port receives path of csv file containing field data.
   */
  public final transient DefaultInputPort<String> input2 = new DefaultInputPort<String>()
  {

    @Override
    public void process(String tuple)
    {
      ICsvMapReader csvReader = null;
      CsvPreference preference = new CsvPreference.Builder('"', fieldDelimiter, lineDelimiter).build();
      InputStream is = null;
      Map<String, Object> fieldValueMapping = new HashMap<String, Object>();
      try {
        is = new FileInputStream(tuple);
      }
      catch (FileNotFoundException ex) {
        logger.error("File Not found", ex);
      }
      BufferedReader br = null;
      try {
        br = new BufferedReader(new InputStreamReader(is, inputEncoding));
      }
      catch (UnsupportedEncodingException ex) {
        logger.error("Encoding not supported", ex);
      }

      csvReader = new CsvMapReader(br, preference);

      try {
        // First line of csv file.
        String[] header = csvReader.getHeader(true);
        int len = header.length;
        if (len > properties.length) {
          logger.debug("More column values in csv file than user supplied field properties.");
        }
        else if (len < properties.length) {
          logger.debug("Less column values in csv file than user supplied field properties.");
        }
        else {
          for (int i = 0; i < len; i++) {
            fieldValueMapping.put(properties[i], header[i]);
            output.emit(fieldValueMapping);
          }
          arrayMaps.add(fieldValueMapping);
          while (true) {
            fieldValueMapping = csvReader.read(properties, processors);
            output.emit(fieldValueMapping);
            if (fieldValueMapping == null) {
              break;
            }
            arrayMaps.add(fieldValueMapping);

          }
          logger.info("arrayMaps is {}", arrayMaps.toString());

        }
      }
      catch (IOException ex) {
        DTThrowable.rethrow(ex);
      }
      finally {
        if (csvReader != null) {
          try {
            csvReader.close();
          }
          catch (IOException ex) {
            DTThrowable.rethrow(ex);
          }
        }
      }

    }

  };

  @Override
  public void setup(OperatorContext context)
  {
    int countKeyValue = listKeyValue.size();
    properties = new String[countKeyValue];
    processors = new CellProcessor[countKeyValue];
    initialise(properties, processors);
  }

  // Still need to handle unknown data types.
  public void initialise(String[] properties, CellProcessor[] processors)
  {
    for (int i = 0; i < listKeyValue.size(); i++) {
      INPUT_TYPE type = listKeyValue.get(i).type;
      properties[i] = listKeyValue.get(i).name;
      if (type == INPUT_TYPE.DOUBLE) {
      }
      else if (type == INPUT_TYPE.INTEGER) {
        processors[i] = new Optional(new ParseInt());
      }
      else if (type == INPUT_TYPE.FLOAT) {
        processors[i] = new Optional(new ParseDouble());
      }
      else if (type == INPUT_TYPE.LONG) {
        processors[i] = new Optional(new ParseLong());
      }
      else if (type == INPUT_TYPE.SHORT) {
        processors[i] = new Optional(new ParseInt());
      }
      else if (type == INPUT_TYPE.STRING) {
        processors[i] = new Optional();
      }
      else if (type == INPUT_TYPE.CHARACTER) {
        processors[i] = new Optional(new ParseChar());
      }
      else if (type == INPUT_TYPE.BOOLEAN) {
        processors[i] = new Optional(new ParseChar());
      }
      else if (type == INPUT_TYPE.DATE) {
        processors[i] = new Optional(new ParseDate("dd/MM/yyyy"));
      }
      else {
        type = INPUT_TYPE.UNKNOWN;
      }
    }

  }

  @Override
  public void teardown()
  {
    super.teardown(); //To change body of generated methods, choose Tools | Templates.
  }

  public void setType(INPUT_TYPE type)
  {
    this.type = type;
  }

  public INPUT_TYPE getType()
  {
    return type;
  }

  public static class Field
  {
    String name;
    INPUT_TYPE type;

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public INPUT_TYPE getType()
    {
      return type;
    }

    public void setType(INPUT_TYPE type)
    {
      this.type = type;
    }

  }

  public ArrayList<Field> getListKeyValue()
  {
    return listKeyValue;
  }

  public void setListKeyValue(ArrayList<Field> listKeyValue)
  {
    this.listKeyValue = listKeyValue;
  }

  public String getLineDelimiter()
  {
    return lineDelimiter;
  }

  public void setLineDelimiter(String lineDelimiter)
  {
    this.lineDelimiter = lineDelimiter;
  }

  public int getFieldDelimiter()
  {
    return fieldDelimiter;
  }

  public void setFieldDelimiter(int fieldDelimiter)
  {
    this.fieldDelimiter = fieldDelimiter;
  }

  private static final Logger logger = LoggerFactory.getLogger(Parser.class);

}
