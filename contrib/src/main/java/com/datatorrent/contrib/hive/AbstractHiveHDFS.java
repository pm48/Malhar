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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.lib.db.AbstractStoreOutputOperator;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractHiveHDFS<T,S extends HiveMetaStore> extends AbstractStoreOutputOperator<T, HiveMetaStore>
{
  protected transient FSDataOutputStream fsOutput;
  protected transient FileSystem fs;
  private long currentWindowId;
  private long committedWindowId = Stateless.WINDOW_ID;

  protected long totalBytesWritten = 0;
  protected boolean append = true;
  protected int bufferSize = 0;
  private static final Logger logger = LoggerFactory.getLogger(AbstractHiveHDFS.class);
  private transient String appId;
  private transient int operatorId;
  protected Statement stmt;

  public AbstractHiveHDFS(){
    this.setStore((S) new HiveMetaStore());
  }

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple);
    }

  };

  /**
   * Function to process each incoming tuple
   *
   * @param t incoming tuple
   */
  @Override
  public void processTuple(T tuple){
     //Minimize duplicated data in the atleast once case
    if(committedWindowId >= currentWindowId) {
      return;
    }
    try {
      fsOutput.write(getBytesForTuple(tuple));
    }
    catch (IOException ex) {
      logger.debug(AbstractHiveHDFS.class.getName()+ ex);
    }
  }

   protected void openFile(Path filepath) throws IOException
  {
    if (fs.exists(filepath)) {
        fs.delete(filepath, true);
        logger.debug("deleting {} ", filepath);
      }
    else {
      fsOutput = fs.create(filepath);
      logger.debug("creating {} ", filepath);
    }

  }

  protected void closeFile() throws IOException
  {
    if (fsOutput != null) {
      fsOutput.close();
      fsOutput = null;
    }
  }

  /**
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    appId = context.getValue(DAG.APPLICATION_ID);
    //store.setFilepath("/"+appId);
    //Minimize duplicated data in the atleast once case
    if(committedWindowId >= currentWindowId) {
      return;
    }
    try {
      fs = FileSystem.newInstance(new Path(store.filepath).toUri(), new Configuration());
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    operatorId = context.getId();
    //Get the last completed window.
    committedWindowId = store.getCommittedWindowId(appId, operatorId);
    logger.debug("AppId {} OperatorId {}", appId, operatorId);
    logger.debug("Committed window id {}", committedWindowId);
  }

  @Override
  public void teardown()
  {
    try {
      closeFile();
      if (fs != null) {
        fs.close();
      }
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    fs = null;
    append = false;
  }


  /**
   * Append to existing file. Default is true.
   *
   * @param append
   * This specifies if there exists a file with same name, then should the operator append to the existing file
   */
  public void setAppend(boolean append)
  {
    this.append = append;
  }

  /**
   * Bytes are written to the underlying file stream once they cross this size.<br>
   * Use this parameter if the file system used does not provide sufficient buffering. HDFS does buffering (even though
   * another layer of buffering on top appears to help) but other file system abstractions may not. <br>
   *
   * @param bufferSize
   */
  public void setBufferSize(int bufferSize)
  {
    this.bufferSize = bufferSize;
  }

   @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    try {
      fsOutput = fs.create(new Path(store.filepath));
    }
    catch (IOException ex) {
      logger.info(AbstractHiveHDFS.class.getName() + ex);
    }
    this.currentWindowId = windowId;
    logger.debug("Committed window {}, current window {}", committedWindowId, currentWindowId);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    try {
       if (fsOutput != null) {
      fsOutput.close();
      fsOutput = null;
    }
    }
    catch (IOException ex) {
      logger.debug(AbstractHiveHDFS.class.getName() + ex);
    }
    processFile();
    //This window is done so write it to the database.
    if(committedWindowId < currentWindowId) {

      store.storeCommittedWindowId(appId, operatorId, currentWindowId);
      committedWindowId = currentWindowId;
    }

  }


  private void processFile()
  {
   String command = getInsertCommand(store.getFilepath());
   try {
   stmt = store.getConnection().createStatement();
   stmt.execute(command);
   }
    catch (SQLException ex) {
      logger.info(AbstractHiveHDFS.class.getName() + " " + ex.getMessage());
    }

  }

  /**
   * This function returns the byte array for the given tuple.
   * @param t The tuple to convert into a byte array.
   * @return The byte array for a given tuple.
   */
  protected abstract byte[] getBytesForTuple(T tuple);
  @Nonnull
  protected abstract String getInsertCommand(String filepath);

  protected abstract void setTableparams(T tuple);

}
