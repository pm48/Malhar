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

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.DTThrowable;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import javax.validation.constraints.Min;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * An implementation of FS Writer that writes text files to hdfs which are inserted
 * into hive on committed window callback.
 */
public class FSRollingOutputOperator<T> extends AbstractFileOutputOperator<T> implements CheckpointListener
{
  private transient String outputFileName;
  protected MutableInt partNumber;
  protected HashMap<String, Long> mapFilenames = new HashMap<String, Long>();
  protected HashMap<String, String> mapPartition = new HashMap<String, String>();
  // Hdfs block size which can be set as a property by user.
  private static final int MAX_LENGTH = 66060288;
  private static final Logger logger = LoggerFactory.getLogger(FSRollingOutputOperator.class);
  protected long windowIDOfCompletedPart = Stateless.WINDOW_ID;
  //protected String partition;
  protected long committedWindowId = Stateless.WINDOW_ID;
  private boolean isEmptyWindow;
  private int countEmptyWindow;
  private String partition;
  //This variable is user configurable.
  @Min(0)
  private long maxWindowsWithNoData = 100;
  @NonNull
  private Converter converter;

  public Converter getConverter()
  {
    return converter;
  }

  public void setConverter(Converter converter)
  {
    this.converter = converter;
  }

  public FSRollingOutputOperator()
  {
    countEmptyWindow = 0;
    setMaxLength(MAX_LENGTH);
  }

  public String getHDFSRollingLastFile()
  {
    Iterator<String> iterFileNames = this.openPart.keySet().iterator();
    String lastFile = null;
    if (iterFileNames.hasNext()) {
      lastFile = iterFileNames.next();
      partNumber = this.openPart.get(lastFile);
    }
    return getPartFileName(lastFile,
                           partNumber.intValue());
  }

  public long getMaxWindowsWithNoData()
  {
    return maxWindowsWithNoData;
  }

  public void setMaxWindowsWithNoData(long maxWindowsWithNoData)
  {
    this.maxWindowsWithNoData = maxWindowsWithNoData;
  }

  @Override
  public void endWindow()
  {
    if (isEmptyWindow) {
      countEmptyWindow++;
    }
    if (countEmptyWindow >= maxWindowsWithNoData) {
      logger.debug("empty window count is max.");
      String lastFile = getHDFSRollingLastFile();
      logger.debug("last file not moved");
      rotateCall(lastFile);
      countEmptyWindow = 0;
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    outputFileName = File.separator + context.getId() + "-" + "transactions.out.part";
    isEmptyWindow = true;
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    isEmptyWindow = true;
    windowIDOfCompletedPart = windowId;
  }
  /*
   * To be implemented by the user, giving a default implementation for one partition column here.
   * Partitions array will get all hive partitions in it.
   * MapFilenames has mapping from completed file to windowId in which it got completed.
   *
   */

  @Override
  protected void rotateHook(String finishedFile)
  {
    mapFilenames.put(finishedFile, windowIDOfCompletedPart);
    logger.debug("finishedFile is {}", finishedFile);
  }

  /*
   * To be implemented by the user, giving a default implementation for one partition column here.
   */
  @Override
  protected String getFileName(T tuple)
  {
    HivePartition hivePartition = new HivePartition();
    partition = hivePartition.getHivePartition(tuple);
    String output = null;
    if (partition != null) {
      output = File.separator + partition + outputFileName;
    }
    else {
      output = outputFileName;
    }
    return output;
  }

  /*
   * Implement this function according to tuple you want to pass in Hive.
   */
  @Override
  protected byte[] getBytesForTuple(T tuple)
  {
    StringConverter converter = new StringConverter();
    String output = converter.getTuple(tuple.toString());
    return output.getBytes();
  }

  /*
   * Moving completed files into hive on committed window callback.
   * Criteria for moving them is that the windowId in which they are completed
   * should be less than committed window.
   */
  @Override
  public void committed(long windowId)
  {
    committedWindowId = windowId;
    Iterator<String> iter = mapFilenames.keySet().iterator();
    while (iter.hasNext()) {
      String fileMoved = iter.next();
      long window = mapFilenames.get(fileMoved);
      String[] output = fileMoved.split("/");
      if (output.length > 1) {
        mapPartition.put(output[2], output[1]);
      }
      else {
        mapPartition.put(output[1], null);
      }
      logger.debug("partition is" + output[1]);
      logger.debug("file is" + output[2]);
      if (committedWindowId >= window) {
        outputPort.emit(mapPartition);
        iter.remove();
        mapPartition.clear();
      }

    }
  }

  /**
   * The output port that will emit tuple into DAG.
   */
  public final transient DefaultOutputPort<HashMap<String, String>> outputPort = new DefaultOutputPort<HashMap<String, String>>();


  /*
   *This method can be used for debugging purposes.
   */
  @Override
  public void checkpointed(long windowId)
  {
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
