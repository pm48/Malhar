/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.parser;

import com.datatorrent.lib.parser.Parser.Field;
import com.datatorrent.lib.parser.Parser.INPUT_TYPE;
import java.util.ArrayList;


public class ParserTest
{
  public static void main(String[] args){
    Parser<String> parser = new Parser<String>();
    parser.setFieldDelimiter(',');
    parser.setLineDelimiter("\n");
    ArrayList<Parser.Field> listKeyValue = new ArrayList<Parser.Field>();
    Field field1 = new Field();
    field1.setName("Eid");
    field1.setType(INPUT_TYPE.INTEGER);
    listKeyValue.add(field1);
    Field field2 = new Field();
    field2.setName("Name");
    field2.setType(INPUT_TYPE.STRING);
    listKeyValue.add(field2);
    Field field3 = new Field();
    field3.setName("Salary");
    field3.setType(INPUT_TYPE.LONG);
    listKeyValue.add(field3);
    parser.setListKeyValue(listKeyValue);
    parser.setup(null);
    parser.input1.process("123,prerna,567777\n321,abhinav,7777000");
   // parser.input2.process("/tmp/parse.txt");

  }
}
