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
package com.datatorrent.lib.io.SMTP;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.HDHT;
import com.datatorrent.contrib.hdht.HDHTWalManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.mail.*;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SMTPWriter extends BaseOperator implements CheckpointListener, SMTP.Writer
{
   public enum RecipientType
  {
    TO, CC, BCC
  }

  private static final Logger LOG = LoggerFactory.getLogger(SmtpOutputOperator.class);
  @NotNull
  private String subject;
  @NotNull
  private String content;
  @NotNull
  private String from;

  private Map<String, String> recipients = Maps.newHashMap();

  private int smtpPort = 587;
  @NotNull
  private String smtpHost;
  private String smtpUserName;
  private String smtpPassword;
  private String contentType = "text/plain";
  private boolean useSsl = false;
  private boolean setupCalled = false;

  protected transient Properties properties = System.getProperties();
  protected transient Authenticator auth;
  protected transient Session session;
  protected transient Message message;
  private long currentWindowId;
  private transient long lastFlushWindowId;
  private final transient HashMap<Long, Bucket> buckets = Maps.newHashMap();
  @VisibleForTesting
  protected transient ExecutorService writeExecutor;
  private volatile transient Throwable writerError;

  private int maxFileSize = 128 * 1024 * 1024; // 128m
  private int maxWalFileSize = 64 * 1024 * 1024;
  private int flushSize = 1000000;
  private int flushIntervalCount = 120;

  private final HashMap<Long, WaLMeta> walMeta = Maps.newHashMap();
  private transient OperatorContext context;

   /**
   * This is the port which receives the tuples that will be output to an smtp server.
   */
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object t)
    {
      try {
        String mailContent = content.replace("{}", t.toString());
        message.setContent(mailContent, contentType);
        String bucketKey = message.hashCode(); // Still need to decide on this.
        Bucket bucket = getBucket(bucketKey);
        // Decide on slice key , value can be message converted to byte array.
        bucket.wal.append( );
        bucket.writeCache.add(message);
        Transport.send(message);
      }
      catch (MessagingException ex) {
        LOG.error("Something wrong with sending email.", ex);
      }
    }
  };


   private static class Bucket
  {
    private long bucketKey;
    // Messages that were modified and written to WAL, but not yet persisted, by checkpoint
    private List<Message> writeCache = new ArrayList<Message>();
    private final LinkedHashMap<Long, List<Message>> checkpointedWriteCache = Maps.newLinkedHashMap();
    public HashMap<Long, HDHTWalManager.WalPosition> walPositions = Maps.newLinkedHashMap();
    private List<Message> committedWriteCache = new ArrayList<Message>();
    // Messages that are being flushed to data files
    private List<Message> frozenWriteCache = new ArrayList<Message>();
    private HDHTWalManager wal;
    private long committedLSN;
    public HDHTWalManager.WalPosition recoveryStartWalPosition;
  }

  /* Holds current file Id for WAL and current recoveryEndWalOffset for WAL */
  private static class WalMeta
  {
    /* The current WAL file and recoveryEndWalOffset */
    // Window Id which is written to the WAL.
    public long windowId;

    // Checkpointed WAL position.
    HDHTWalManager.WalPosition cpWalPosition;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    writeExecutor = Executors.newSingleThreadScheduledExecutor(new NameableThreadFactory(this.getClass().getSimpleName() + "-Writer"));
    this.context = context;
    setupCalled = true;
    reset();
  }


  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
  }

  @Override
  public void put(long bucketKey, commi key, byte[] value) throws IOException
  {
  }

  @Override
  public byte[] getUncommitted(long bucketKey, Slice key)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.currentWindowId = windowId;
  }

  @Override
  public void endWindow()
  {
  }


  @Override
  public void teardown()
  {
  }


}
