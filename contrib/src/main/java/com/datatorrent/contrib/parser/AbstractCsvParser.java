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
package com.datatorrent.contrib.parser;

import java.util.ArrayList;

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
import org.supercsv.cellprocessor.*;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.*;

/*
 *  This is a base implementation of Delimited data parser which can be extended to output
 *  field values in desired data structure.
 *  Assumption is that each field in the delimited data should map to a simple java type.
 *  Delimited records can be supplied in file or message based sources like kafka.
 *  User can specify the name of the field , data type of the field as list of key value pairs
 *  and delimiter as properties on the parsing operator.
 *  Other properties to be specified
 *        - Input Stream encoding - default value should be UTF-8
 *        - End of line character - default should be ‘\r\n’
 *
 * @param <T> This is the output tuple type.
 */
public abstract class AbstractCsvParser<T> extends BaseOperator
{
  // List of key value pairs which has name of the field as key , data type of the field as value.
  @NotNull
  protected ArrayList<Field> fields;

  protected String inputEncoding;
  @NotNull
  protected int fieldDelimiter;
  protected String lineDelimiter;
  protected transient String[] properties;
  protected transient CellProcessor[] processors;
  protected boolean isHeader;

  public enum FIELD_TYPE
  {
    BOOLEAN, DOUBLE, INTEGER, FLOAT, LONG, SHORT, CHARACTER, STRING, DATE
  };

  @NotNull
  FIELD_TYPE type;

  public AbstractCsvParser()
  {
    fieldDelimiter = ',';
    inputEncoding = "UTF8";
    lineDelimiter = "\r\n";
    type = FIELD_TYPE.STRING;
    isHeader = false;
  }

  /**
   * Output port that emits value of the fields.
   * Output data type can be configured in the implementation of this operator.
   */
  public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();

  /**
   * This input port receives byte array as tuple.
   */
  public final transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] tuple)
    {
      ICsvReader csvReader = null;
      CsvPreference preference = new CsvPreference.Builder('"', fieldDelimiter, lineDelimiter).build();
      InputStream in;
      BufferedReader br = null;
      try {
        in = new ByteArrayInputStream(tuple);
        br = new BufferedReader(new InputStreamReader(in, inputEncoding));
      }
      catch (UnsupportedEncodingException ex) {
        logger.error("Encoding not supported", ex);
        DTThrowable.rethrow(ex);
      }
      csvReader = getReader(br, preference);
      try {
        if (isHeader) {
          String[] header = csvReader.getHeader(true);
          int len = header.length;
          for (int i = 0; i < len; i++) {
            logger.debug("header is {}", header[i]);
            T headerData = (T)header[i];
            output.emit(headerData);
          }
        }

        while (true) {
          T data = readData(properties, processors);
          if (data == null) {
            break;
          }
          logger.debug("data in loop is {}", data.toString());
          output.emit(data);
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

  @Override
  public void setup(OperatorContext context)
  {
    int countKeyValue = fields.size();
    properties = new String[countKeyValue];
    processors = new CellProcessor[countKeyValue];
    initialise(properties, processors);
  }

  // Initialise the properties and processors.
  public void initialise(String[] properties, CellProcessor[] processors)
  {
    for (int i = 0; i < fields.size(); i++) {
      FIELD_TYPE type = fields.get(i).type;
      properties[i] = fields.get(i).name;
      if (type == FIELD_TYPE.DOUBLE) {
        processors[i] = new Optional(new ParseDouble());
      }
      else if (type == FIELD_TYPE.INTEGER) {
        processors[i] = new Optional(new ParseInt());
      }
      else if (type == FIELD_TYPE.FLOAT) {
        processors[i] = new Optional(new ParseDouble());
      }
      else if (type == FIELD_TYPE.LONG) {
        processors[i] = new Optional(new ParseLong());
      }
      else if (type == FIELD_TYPE.SHORT) {
        processors[i] = new Optional(new ParseInt());
      }
      else if (type == FIELD_TYPE.STRING) {
        processors[i] = new Optional();
      }
      else if (type == FIELD_TYPE.CHARACTER) {
        processors[i] = new Optional(new ParseChar());
      }
      else if (type == FIELD_TYPE.BOOLEAN) {
        processors[i] = new Optional(new ParseChar());
      }
      else if (type == FIELD_TYPE.DATE) {
        processors[i] = new Optional(new ParseDate("dd/MM/yyyy"));
      }
    }

  }

  @Override
  public void teardown()
  {
    super.teardown();
  }

  /**
   * Any concrete class derived from AbstractParser has to implement this method.
   * It returns an instance of specific CsvReader required to read field values into a specific data type.
   *
   * @param br
   * @param preference
   * @return CsvReader
   */
  public abstract ICsvReader getReader(BufferedReader br, CsvPreference preference);

  /**
   * Any concrete class derived from AbstractParser has to implement this method.
   * It returns the specific data type in which field values are being read to.
   *
   * @param properties
   * @param processors
   */
  public abstract T readData(String[] properties, CellProcessor[] processors);

  public static class Field
  {
    String name;
    FIELD_TYPE type;

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public FIELD_TYPE getType()
    {
      return type;
    }

    public void setType(FIELD_TYPE type)
    {
      this.type = type;
    }

  }

  public void setType(FIELD_TYPE type)
  {
    this.type = type;
  }

  public FIELD_TYPE getType()
  {
    return type;
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

  public boolean isIsHeader()
  {
    return isHeader;
  }

  public void setIsHeader(boolean isHeader)
  {
    this.isHeader = isHeader;
  }

  public ArrayList<Field> getFields()
  {
    return fields;
  }

  public void setFields(ArrayList<Field> fields)
  {
    this.fields = fields;
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractCsvParser.class);

}
