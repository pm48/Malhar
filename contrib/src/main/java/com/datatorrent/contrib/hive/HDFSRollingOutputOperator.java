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

import java.io.File;
import java.util.Iterator;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.io.fs.AbstractFSWriter;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.DTThrowable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * An implementation of FS Writer that writes text files to hdfs which are inserted
 * into hive on committed window callback.
 */
public class HDFSRollingOutputOperator<T> extends AbstractFSWriter<T>
{
  private transient String outputFileName;
  protected MutableInt partNumber;
  protected String lastFile;
  protected AbstractHiveHDFS<T> hive;
  // Hdfs block size which can be set as a property by user.
  private static final int MAX_LENGTH = 66060288;
  private static final Logger logger = LoggerFactory.getLogger(HDFSRollingOutputOperator.class);

  public HDFSRollingOutputOperator()
  {
    setMaxLength(MAX_LENGTH);
  }

  public String getHDFSRollingLastFile()
  {
    Iterator<String> iterFileNames = this.openPart.keySet().iterator();
    if (iterFileNames.hasNext()) {
      lastFile = iterFileNames.next();
      partNumber = this.openPart.get(lastFile);
    }
    return getPartFileName(lastFile,
                           partNumber.intValue());
  }

  public FileSystem getFileSystem()
  {
    return fs;
  }

  public boolean isHDFSLocation()
  {
    if ((fs instanceof LocalFileSystem) || (fs instanceof RawLocalFileSystem)) {
      return false;
    }
    else if (fs.getScheme().equalsIgnoreCase("hdfs")) {
      return true;
    }
    throw new UnsupportedOperationException("This operation is not supported");
  }

  @Override
  public void setup(OperatorContext context)
  {
    outputFileName = File.separator + context.getId() + "-" + "transactions.out.part";
    super.setup(context);
  }

  /*
   * To be implemented by the user, giving a default implementation for one partition column here.
   * Partitions array will get all hive partitions in it.
   * MapFilenames has mapping from completed file to windowId in which it got completed.
   * MapFilePartition stores mapping from completed files to their corresponding hive partitions.
   */
  @Override
  protected void rotateHook(String finishedFile)
  {
    hive.mapFilenames.put(finishedFile, hive.windowIDOfCompletedPart);
    String[] partitions = finishedFile.split("/");

    for (int i = 0; i < partitions.length - 1; i++) {
      logger.info("partitions are {}", partitions[i]);
    }

    hive.partition = partitions[1].replace("\"", "'");
    hive.mapFilePartition.put(finishedFile, hive.partition);
  }

  /*
   * To be implemented by the user, giving a default implementation for one partition column here.
   */
  @Override
  protected String getFileName(T tuple)
  {
    String temp = hive.partition.replace("'", "\"");
    String output = File.separator + temp + outputFileName;
    return output;
  }

  /*
   * Implement this function according to tuple you want to pass in Hive.
   */
  @Override
  protected byte[] getBytesForTuple(T tuple)
  {
    String hiveTuple = hive.getHiveTuple(tuple);
    return hiveTuple.getBytes();
  }

  protected void rotateCall(String lastFile)
  {
    try {
      this.rotate(lastFile);
    }
    catch (IllegalArgumentException ex) {
      logger.debug(ex.getMessage());
      DTThrowable.rethrow(ex);
    }
    catch (IOException ex) {
      logger.debug(ex.getMessage());
      DTThrowable.rethrow(ex);
    }
    catch (ExecutionException ex) {
      logger.debug(ex.getMessage());
      DTThrowable.rethrow(ex);
    }
  }

}
