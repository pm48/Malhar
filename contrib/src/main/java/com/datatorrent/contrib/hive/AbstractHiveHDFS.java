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

import com.datatorrent.api.*;
import java.sql.SQLException;
import java.sql.Statement;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.db.AbstractStoreOutputOperator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.common.util.Slice;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import javax.validation.constraints.Min;
import org.apache.hadoop.fs.Path;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.*;
import java.util.Map;
import org.apache.commons.io.output.ByteArrayOutputStream;

/*
 * An abstract Hive operator which can insert data in ORC/TEXT tables from a file written in hdfs location.
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public abstract class AbstractHiveHDFS<T> extends AbstractStoreOutputOperator<T, HiveStore> implements CheckpointListener, Partitioner<AbstractHiveHDFS<T>>,StreamCodec<T>,Serializable
{
  private static final long serialVersionUID = 1L;
  protected boolean isPartitioned = true;
  protected transient int numPartitions = 3;
  protected String partition;

  public int getNumPartitions()
  {
    return numPartitions;
  }

  public void setNumPartitions(int numPartitions)
  {
    this.numPartitions = numPartitions;
  }

 /* public final transient DefaultInputPort<T> in = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple);
    }

  };*/

  protected KryoSerializableStreamCodec<T> codec;

  @Override
  public Object fromByteArray(Slice fragment)
	{
		return codec.fromByteArray(fragment);
	}

	@Override
  @SuppressWarnings("unchecked")
	public Slice toByteArray(T object)
	{
		return codec.toByteArray(object);
	}

  @Override
  public int getPartition(T o)
  {
    return getHivePartition(o).hashCode();
  }

  @Override
  public Collection<Partition<AbstractHiveHDFS<T>>> definePartitions(Collection<Partition<AbstractHiveHDFS<T>>> partitions, int incrementalCapacity)
  {
    int totalCount = numPartitions;
    Collection<Partition<AbstractHiveHDFS<T>>> newPartitions = Lists.newArrayListWithExpectedSize(totalCount);
    Kryo kryo = new Kryo();
    for (int i = 0; i < totalCount; i++) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      kryo.writeObject(output, this);
      output.close();
      Input lInput = new Input(bos.toByteArray());
      @SuppressWarnings("unchecked")
      AbstractHiveHDFS<T> oper = kryo.readObject(lInput, this.getClass());
      //oper.setHdfsOp(new HDFSRollingOutputOperator<T>());
      // oper.setStore(this.store);
      newPartitions.add(new DefaultPartition<AbstractHiveHDFS<T>>(oper));
    }

    return newPartitions;

  }

  @Override
   public void partitioned(Map<Integer, Partition<AbstractHiveHDFS<T>>> partitions)
   {
   }
  protected long committedWindowId = Stateless.WINDOW_ID;
  private static final Logger logger = LoggerFactory.getLogger(AbstractHiveHDFS.class);
  private transient String appId;
  private transient int operatorId;
  protected HashMap<String, Long> filenames;
  //This variable is user configurable
  @Min(0)
  private transient long maxWindowsWithNoData = 100;

  public long getMaxWindowsWithNoData()
  {
    return maxWindowsWithNoData;
  }

  public void setMaxWindowsWithNoData(long maxWindowsWithNoData)
  {
    this.maxWindowsWithNoData = maxWindowsWithNoData;
  }

  private int countEmptyWindow;
  private boolean isEmptyWindow;
  protected long windowIDOfCompletedPart = Stateless.WINDOW_ID;

  @Nonnull
  protected String tablename;

  public HDFSRollingOutputOperator<T> hdfsOp;



  public HDFSRollingOutputOperator<T> getHdfsOp()
  {
    return hdfsOp;
  }

  public void setHdfsOp(HDFSRollingOutputOperator<T> hdfsOp)
  {
    this.hdfsOp = hdfsOp;
  }

  public AbstractHiveHDFS()
  {
    hdfsOp = new HDFSRollingOutputOperator<T>();
    filenames = new HashMap<String, Long>();
    hdfsOp.hive = this;
    countEmptyWindow = 0;
  }

  @Override
  public void beginWindow(long windowId)
  {
    isEmptyWindow = true;
    windowIDOfCompletedPart = windowId;
  }

  @Override
  public void committed(long windowId)
  {
    committedWindowId = windowId;
    Iterator<String> iter = filenames.keySet().iterator();
    while (iter.hasNext()) {
      String fileMoved = iter.next();
      long window = filenames.get(fileMoved);
      logger.debug("filemoved is {}", fileMoved);
      logger.debug("window is {}", window);
      if (committedWindowId >= window) {
        try {
          logger.debug("path in committed window is" + store.getOperatorpath() + "/" + fileMoved);
          //comments to be added.
          if (hdfsOp.getFileSystem().exists(new Path(store.getOperatorpath() + "/" + fileMoved))) {
            logger.debug("partition is" + partition);
            processHiveFile(fileMoved);
          }
        }
        catch (IOException ex) {
          logger.debug(ex.getMessage());
        }
        iter.remove();
      }

    }
  }

  /*
   *This method can be used for debugging purposes.
   */
  @Override
  public void checkpointed(long windowId)
  {
  }

  public String getTablename()
  {
    return tablename;
  }

  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  /**
   * Function to process each incoming tuple
   *
   * @param tuple incoming tuple
   */
  @Override
  public void processTuple(T tuple)
  {
     if (isPartitioned) {
      partition = getHivePartition(tuple);
    }
    isEmptyWindow = false;
    hdfsOp.input.process(tuple);
  }

  @Override
  public void setup(OperatorContext context)
  {
    appId = context.getValue(DAG.APPLICATION_ID);
    operatorId = context.getId();
    hdfsOp.setFilePath(store.filepath + "/" + appId + "/" + operatorId);
    store.setOperatorpath(store.filepath + "/" + appId + "/" + operatorId);
    super.setup(context);
    hdfsOp.setup(context);
    isEmptyWindow = true;
  }

  @Override
  public void teardown()
  {
    hdfsOp.teardown();
    super.teardown();
  }

  @Override
  public void endWindow()
  {
    hdfsOp.endWindow();
    if (isEmptyWindow) {
      countEmptyWindow++;
    }
    if (countEmptyWindow >= maxWindowsWithNoData) {
      logger.debug("empty window count is max.");
      String lastFile = hdfsOp.getHDFSRollingLastFile();
      try {
        logger.debug("path in end window is" + store.getOperatorpath() + "/" + lastFile);
        if (hdfsOp.getFileSystem().exists(new Path(store.getOperatorpath() + "/" + lastFile))) {
          logger.debug("last file not moved");
          hdfsOp.rotateCall(hdfsOp.lastFile);
        }
      }
      catch (IOException ex) {
        logger.debug(ex.getMessage());
      }
      countEmptyWindow = 0;
    }
  }

  public void processHiveFile(String fileMoved)
  {
    logger.debug("processing {} file", fileMoved);
    String command = getInsertCommand(store.getOperatorpath() + "/" + fileMoved);
    Statement stmt;
    try {
      stmt = store.getConnection().createStatement();
      if(!stmt.execute(command))
        //either throw exception or log error.
        logger.error("Moving file into hive failed");
    }
    catch (SQLException ex) {
      DTThrowable.rethrow(ex);
    }
  }

  public String getHiveTuple(T tuple)
  {
    return tuple.toString() + "\n";
  }

  /*
   * To be implemented by the user
   */
  public abstract String getHivePartition(T tuple);

  protected String getInsertCommand(String filepath)
  {
    String command;
    if (isPartitioned) {
      if (!hdfsOp.isHDFSLocation()) {
        command = "load data local inpath '" + filepath + "' into table " + tablename + " PARTITION " + "(" + partition + ")";
      }
      else {
        command = "load data inpath '" + filepath + "' into table " + tablename + " PARTITION " + "(" + partition + ")";
      }
    }
    else {
      if (!hdfsOp.isHDFSLocation()) {
        command = "load data local inpath '" + filepath + "' into table " + tablename;
      }
      else {
        command = "load data inpath '" + filepath + "' into table " + tablename;
      }
    }
    logger.debug("command is {}", command);

    return command;

  }

}
