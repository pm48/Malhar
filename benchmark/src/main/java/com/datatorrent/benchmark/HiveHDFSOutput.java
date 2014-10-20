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
package com.datatorrent.benchmark;

import com.datatorrent.contrib.hive.AbstractHiveHDFS;
import com.datatorrent.contrib.hive.HiveMetaStore;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveHDFSOutput extends AbstractHiveHDFS<String, HiveMetaStore>
{
  public static final String tableName = "temp";
  private static final Logger logger = LoggerFactory.getLogger("HiveHDFSOutput.class");

  @Override
  protected byte[] getBytesForTuple(String tuple)
  {
    return tuple.getBytes();
  }

  @Override
  protected String getInsertCommand(String filepath)
  {
    return "load data inpath '" + filepath + "' into table " + tableName;
  }

  @Override
  protected void setTableparams(String tuple)
  {
    try {
      stmt.execute("CREATE TABLE IF NOT EXISTS" + tableName + "(col1 " + tuple +" ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n"
              + "COLLECTION ITEMS TERMINATED BY '\n'  \n"
              + "LINES TERMINATED BY '\n'  \n"
              + "STORED AS TEXTFILE ");
    }
    catch (SQLException ex) {
      logger.debug(HiveHDFSOutput.class.getName() + ex);
    }
  }

}