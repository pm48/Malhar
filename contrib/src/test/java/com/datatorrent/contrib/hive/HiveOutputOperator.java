/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.contrib.hive;

import java.util.HashMap;



public class HiveOutputOperator extends AbstractHiveOutputOperator<HashMap,HiveStore>
{
  static String filepath = "/tmp/a.txt";
  private static String INSERT = "load data local inpath '" + filepath + "' into table test";
//  String str = "a=1 b=42 x=abc";
// private static String Insert = "Insert overwrite table test select str_to_map(str,\" \",\"=\")";


  @Override
  protected String getUpdateCommand()
  {
    return INSERT;
  }

  /*@Override
  protected void setStatementParameters(PreparedStatement statement, Integer tuple) throws SQLException
  {
    statement.setInt(1, tuple);
  }*/


}
