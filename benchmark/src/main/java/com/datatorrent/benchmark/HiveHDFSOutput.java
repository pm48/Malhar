/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark;

import com.datatorrent.contrib.hive.AbstractHiveHDFS;
import com.datatorrent.contrib.hive.HiveMetaStore;
import java.sql.SQLException;
import org.apache.log4j.Logger;

public class HiveHDFSOutput extends AbstractHiveHDFS<String, HiveMetaStore>
{
  //map,list
  public static final String tableName = "temp";
  private static final Logger logger = Logger.getLogger("HiveHDFSOutput.class");

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
      stmt.execute("drop table " + tableName);
      stmt.execute("CREATE TABLE IF NOT EXISTS" + tableName + "(col1 tuple) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n"
              + "COLLECTION ITEMS TERMINATED BY '\n'  \n"
              + "LINES TERMINATED BY '\n'  \n"
              + "STORED AS TEXTFILE ");
    }
    catch (SQLException ex) {
      logger.debug(HiveHDFSOutput.class.getName() + ex);
    }
  }

}
