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
import java.util.logging.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.supercsv.cellprocessor.*;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.*;

/*
 *  This is a base implementation of Delimited data parser which can be extended to output
 *  field values in desired data structure.
 *  Assumption is that each field in the delimited data should map to a simple java type.
 *  Delimited records can be supplied in file or message based sources like kafka.
 *  User can specify the name of the field , data type of the field as list of key value pairs or in a hdfs file
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
  protected ArrayList<Field> fields;

  protected String inputEncoding;
  @NotNull
  protected int fieldDelimiter;
  protected String lineDelimiter;
  //User gets an option to specify filename containing name of the field and data type of the field.
  protected String fieldmappingFile;
  //Field and its data type can be separated by a user defined delimiter in the file.
  protected String fieldmappingFileDelimiter;

  protected transient String[] properties;
  protected transient CellProcessor[] processors;
  protected boolean isHeader;
  private transient ICsvReader csvReader;

  public enum FIELD_TYPE
  {
    BOOLEAN, DOUBLE, INTEGER, FLOAT, LONG, SHORT, CHARACTER, STRING, DATE
  };

  @NotNull
  FIELD_TYPE type;

  @NotNull
  private transient ReusableStringReader csvStringReader = new ReusableStringReader();

  public AbstractCsvParser()
  {
    fields = new ArrayList<Field>();
    fieldDelimiter = ',';
    fieldmappingFileDelimiter = ":";
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
      //InputStream in;
      /*BufferedReader br = null;
       try {
       in = new ByteArrayInputStream(tuple);
       br = new BufferedReader(new InputStreamReader(in, inputEncoding));
       }
       catch (UnsupportedEncodingException ex) {
       logger.error("Encoding not supported", ex);
       DTThrowable.rethrow(ex);
       }
       csvReader = getReader(br, preference);*/

      try {
        csvStringReader.open(new String(tuple, inputEncoding));
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

    }

  };

  @Override
  public void setup(OperatorContext context)
  {
    if (fieldmappingFile != null) {
      Configuration conf = new Configuration();
      try {
        FileSystem fs = FileSystem.get(conf);
        Path filepath = new Path(fieldmappingFile);
        if(fs.exists(filepath)){
        BufferedReader bfr = new BufferedReader(new InputStreamReader(fs.open(filepath)));
        String str;

        while ((str = bfr.readLine()) != null) {
          logger.debug("string is {}", str);
          String[] temp = str.split(fieldmappingFileDelimiter);
          Field field = new Field();
          field.setName(temp[0]);
          field.setType(temp[1]);
          fields.add(field);
        }
      }
      else
        {
          logger.debug("File containing fields and their data types does not exist.Please specify the fields and data type through properties of this operator.");
        }
      }
      catch (IOException ex) {
        DTThrowable.rethrow(ex);
      }

    }

      int countKeyValue = fields.size();
      properties = new String[countKeyValue];
      processors = new CellProcessor[countKeyValue];
      initialise(properties, processors);
      CsvPreference preference = new CsvPreference.Builder('"', fieldDelimiter, lineDelimiter).build();
      csvReader = getReader(csvStringReader, preference);

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
    try {
      csvReader.close();
    }
    catch (IOException e) {
      logger.error("Parsing csv error", e);
    }
  }

  /**
   * Any concrete class derived from AbstractParser has to implement this method.
   * It returns an instance of specific CsvReader required to read field values into a specific data type.
   *
   * @param reader
   * @param preference
   * @return CsvReader
   */
  public abstract ICsvReader getReader(ReusableStringReader reader, CsvPreference preference);

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

    public void setType(String type)
    {
      this.type = FIELD_TYPE.valueOf(type);
    }

  }

  public static class ReusableStringReader extends Reader
  {
    private String str;
    private int length;
    private int next = 0;

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException
    {
      ensureOpen();
      if ((off < 0) || (off > cbuf.length) || (len < 0) || ((off + len) > cbuf.length) || ((off + len) < 0)) {
        throw new IndexOutOfBoundsException();
      }
      else if (len == 0) {
        return 0;
      }
      if (next >= length) {
        return -1;
      }
      int n = Math.min(length - next, len);
      str.getChars(next, next + n, cbuf, off);
      next += n;
      return n;
    }

    /**
     * Reads a single character.
     *
     * @return The character read, or -1 if the end of the stream has been reached
     *
     * @exception IOException
     * If an I/O error occurs
     */
    @Override
    public int read() throws IOException
    {
      ensureOpen();
      if (next >= length) {
        return -1;
      }
      return str.charAt(next++);
    }

    @Override
    public boolean ready() throws IOException
    {
      ensureOpen();
      return true;
    }

    @Override
    public void close() throws IOException
    {
      str = null;
    }

    /**
     * Check to make sure that the stream has not been closed
     */
    private void ensureOpen() throws IOException
    {
      if (str == null) {
        throw new IOException("Stream closed");
      }
    }

    public void open(String str) throws IOException
    {
      this.str = str;
      this.length = this.str.length();
      this.next = 0;
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

  public String getFieldmappingFile()
  {
    return fieldmappingFile;
  }

  public void setFieldmappingFile(String fieldmappingFile)
  {
    this.fieldmappingFile = fieldmappingFile;
  }

  public String getFieldmappingFileDelimiter()
  {
    return fieldmappingFileDelimiter;
  }

  public void setFieldmappingFileDelimiter(String fieldmappingFileDelimiter)
  {
    this.fieldmappingFileDelimiter = fieldmappingFileDelimiter;
  }
  private static final Logger logger = LoggerFactory.getLogger(AbstractCsvParser.class);

}
