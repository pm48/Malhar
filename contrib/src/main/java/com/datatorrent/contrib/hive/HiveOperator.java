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

import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.Collection;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.datatorrent.lib.db.AbstractStoreOutputOperator;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.contrib.hive.FSRollingOutputOperator.FilePartitionMapping;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

/*
 * Hive operator which can insert data in txt format in tables/partitions from a file written in hdfs location.
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public class HiveOperator extends AbstractStoreOutputOperator<FilePartitionMapping, HiveStore>
{
  //This Property is user configurable.
  protected ArrayList<String> hivePartitionColumns = new ArrayList<String>();
  protected String partition;
  /**
   * The file system used to write to.
   */
  protected transient FileSystem fs;
  @Nonnull
  protected String tablename;
  @Nonnull
  protected String hivepath;

  @Override
  public void setup(OperatorContext context)
  {
    String appId = context.getValue(DAG.APPLICATION_ID);
    store.setOperatorpath(store.filepath + "/" + appId + "/" + context.getId());
    //Getting required file system instance.
    try {
      fs = getFSInstance();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    super.setup(context);
  }

  /**
   * Override this method to change the FileSystem instance that is used by the operator.
   * This method is mainly helpful for unit testing.
   * @return A FileSystem object.
   * @throws IOException
   */
  protected FileSystem getFSInstance() throws IOException
  {
    FileSystem tempFS = FileSystem.newInstance(new Path(store.filepath).toUri(), new Configuration());

    if(tempFS instanceof LocalFileSystem)
    {
      tempFS = ((LocalFileSystem) tempFS).getRaw();
    }

    return tempFS;
  }

  /**
   * Function to process each incoming tuple
   * This can be overridden by user for multiple partition columns.
   * Giving an implementation for one partition column.
   *
   * @param tuple incoming tuple which has filename and hive partition.
   */
  @Override
  public void processTuple(FilePartitionMapping tuple)
  {
    String fileMoved = tuple.getFilename();
    partition = tuple.getPartition();
    processHiveFile(fileMoved);

  }

  public void processHiveFile(String fileMoved)
  {
    logger.info("processing {} file", fileMoved);
    String command = getInsertCommand(fileMoved);
    if(command!=null){
    Statement stmt;
    try {
      stmt = store.getConnection().createStatement();
      stmt.execute(command);
    }
    catch (SQLException ex) {
      throw new RuntimeException("Moving file into hive failed" + ex);
    }
  }
  }

  /*
   * User can specify multiple partitions here, giving a default implementation for one partition column here.
   */
  protected String getInsertCommand(String filepath)
  {
    String command = null;
    Path hivefilepath = new Path(hivepath + Path.SEPARATOR + filepath);
    filepath = store.getFilepath()+ Path.SEPARATOR + filepath;
    logger.info("filepath is {}",filepath);
    logger.info("hivefilepath is {}",hivefilepath);
    try{
     if (fs.exists(new Path(filepath))) {
        if (partition != null) {
          partition = getHivePartitionColumns().get(0) + "='" + partition + "'";
          if(fs.exists(hivefilepath)){
            command = "load data inpath '" + filepath + "' OVERRIDE into table " + tablename + " PARTITION" + "( " + partition + " )";
          }
          else
          command = "load data inpath '" + filepath + "' into table " + tablename + " PARTITION" + "( " + partition + " )";
        }
        else {
          if(fs.exists(hivefilepath)){
           command = "load data local inpath '" + filepath + "' OVERRIDE into table " + tablename;
          }
          else
          command = "load data local inpath '" + filepath + "' into table " + tablename;
        }
    }
    }
    catch (IOException e) {
          throw new RuntimeException(e);
        }
    logger.info("command is {}", command);
    return command;

  }

  public ArrayList<String> getHivePartitionColumns()
  {
    return hivePartitionColumns;
  }

  public void setHivePartitionColumns(ArrayList<String> hivePartitionColumns)
  {
    this.hivePartitionColumns = hivePartitionColumns;
  }

  public String getTablename()
  {
    return tablename;
  }

  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  public String getHivepath()
  {
    return hivepath;
  }

  public void setHivepath(String hivepath)
  {
    this.hivepath = hivepath + "/" + tablename;
  }

  private static final Logger logger = LoggerFactory.getLogger(HiveOperator.class);

}
