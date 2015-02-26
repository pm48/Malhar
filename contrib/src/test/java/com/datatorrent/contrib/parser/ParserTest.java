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

import com.datatorrent.contrib.parser.AbstractCsvParser.FIELD_TYPE;
import com.datatorrent.contrib.parser.AbstractCsvParser.Field;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;

public class ParserTest
{
  @Test
  public void TestParserWithHeader()
  {
    CsvParser parser = new CsvParser();
    parser.setFieldDelimiter(',');
    parser.setLineDelimiter("\n");
    parser.setIsHeader(true);
    ArrayList<CsvParser.Field> fields = new ArrayList<CsvParser.Field>();
    Field field1 = new Field();
    field1.setName("Eid");
    field1.setType(FIELD_TYPE.INTEGER);
    fields.add(field1);
    Field field2 = new Field();
    field2.setName("Name");
    field2.setType(FIELD_TYPE.STRING);
    fields.add(field2);
    Field field3 = new Field();
    field3.setName("Salary");
    field3.setType(FIELD_TYPE.LONG);
    fields.add(field3);
    parser.setFields(fields);
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    parser.output.setSink(sink);
    parser.setup(null);
    String input = "Eid,Name,Salary\n123,xyz,567777\n321,abc,7777000\n456,pqr,5454545454";
    parser.input.process(input.getBytes());
    Assert.assertEquals("Tuples read should be same ", 6, sink.collectedTuples.size());
    Assert.assertEquals("Eid", sink.collectedTuples.get(0));
    Assert.assertEquals("Name", sink.collectedTuples.get(1));
    Assert.assertEquals("Salary", sink.collectedTuples.get(2));
    Assert.assertEquals("{Name=xyz, Salary=567777, Eid=123}", sink.collectedTuples.get(3).toString());
    Assert.assertEquals("{Name=abc, Salary=7777000, Eid=321}", sink.collectedTuples.get(4).toString());
    Assert.assertEquals("{Name=pqr, Salary=5454545454, Eid=456}", sink.collectedTuples.get(5).toString());
    sink.clear();

  }

  @Test
  public void TestParserWithoutHeader()
  {
    CsvParser parser = new CsvParser();
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    parser.output.setSink(sink);
    parser.setFieldDelimiter(',');
    parser.setLineDelimiter("\n");
    ArrayList<CsvParser.Field> fields = new ArrayList<CsvParser.Field>();
    Field field1 = new Field();
    field1.setName("Eid");
    field1.setType(FIELD_TYPE.INTEGER);
    fields.add(field1);
    Field field2 = new Field();
    field2.setName("Name");
    field2.setType(FIELD_TYPE.STRING);
    fields.add(field2);
    Field field3 = new Field();
    field3.setName("Salary");
    field3.setType(FIELD_TYPE.LONG);
    fields.add(field3);
    parser.setFields(fields);
    parser.setIsHeader(false);
    parser.setup(null);
    String input = "123,xyz,567777\n321,abc,7777000\n456,pqr,5454545454";
    parser.input.process(input.getBytes());
    Assert.assertEquals("Tuples read should be same ", 3, sink.collectedTuples.size());
    Assert.assertEquals("{Name=xyz, Salary=567777, Eid=123}", sink.collectedTuples.get(0).toString());
    Assert.assertEquals("{Name=abc, Salary=7777000, Eid=321}", sink.collectedTuples.get(1).toString());
    Assert.assertEquals("{Name=pqr, Salary=5454545454, Eid=456}", sink.collectedTuples.get(2).toString());
    sink.clear();
  }

}