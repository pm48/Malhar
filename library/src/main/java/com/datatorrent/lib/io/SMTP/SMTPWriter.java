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
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.HDHT;
import com.datatorrent.contrib.hdht.HDHTFileAccess;
import com.datatorrent.contrib.hdht.HDHTFileAccess.HDSFileReader;
import com.datatorrent.contrib.hdht.HDHTFileAccess.HDSFileWriter;
import com.datatorrent.contrib.hdht.HDHTReader.BucketFileMeta;
import com.datatorrent.contrib.hdht.HDHTReader.DefaultKeyComparator;
import com.datatorrent.contrib.hdht.HDHTWalManager;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Sets;
import java.io.OutputStream;
import org.apache.commons.io.IOUtils;

public abstract class SMTPWriter extends BaseOperator implements CheckpointListener, HDHT.Writer
{
  public static final String FNAME_META = "_META";

  @Override
  public void put(long bucketKey, Slice key, byte[] value) throws IOException
  {
    Bucket bucket = getBucket(bucketKey);
    bucket.wal.append(key, value);
    bucket.writeCache.put(key, value);
  }


  protected final transient Kryo kryo = new Kryo();
  @NotNull
  protected Comparator<Slice> keyComparator = new DefaultKeyComparator();

  private BucketMeta loadBucketMeta(long bucketKey)
  {
    BucketMeta bucketMeta = null;
    try {
      InputStream is = store.getInputStream(bucketKey, FNAME_META);
      bucketMeta = (BucketMeta)kryo.readClassAndObject(new Input(is));
      is.close();
    }
    catch (IOException e) {
      bucketMeta = new BucketMeta(keyComparator);
    }
    return bucketMeta;
  }

  private Comparator<? super Slice> getKeyComparator()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  public enum RecipientType
  {
    TO, CC, BCC
  }

  @NotNull
  protected static HDHTFileAccess store;
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
  private final transient HashMap<Long, BucketMeta> metaCache = Maps.newHashMap();

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
  private int index = 0;
  private transient OperatorContext context;

  private final HashMap<Long, WalMeta> walMeta = Maps.newHashMap();

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
        index++;
        long bucketKey = index;
        // Decide on slice key , value can be message converted to byte array.
        byte[] key = getKeyBytes(message);
        byte[] value = message.toString().getBytes();
        processMessage(bucketKey, new Slice(key), value);
        Transport.send(message);
      }
      catch (MessagingException ex) {
        LOG.error("Something wrong with sending email.", ex);
      }

    }

  };

  protected void processMessage(long bucketKey, Slice slice, byte[] value)
  {
    try {
      put(bucketKey,slice,value);
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
  }

  protected abstract byte[] getKeyBytes(Message message);

  /**
   * Meta data about bucket, persisted in store
   * Flushed on compaction
   */
  public static class BucketMeta
  {
    protected BucketMeta(Comparator<Slice> cmp)
    {
      files = new TreeMap<Slice, BucketFileMeta>(cmp);
    }

    @SuppressWarnings("unused")
    private BucketMeta()
    {
      // for serialization only
      files = null;
    }

    protected final transient Kryo kryo = new Kryo();
    @NotNull
    protected Comparator<Slice> keyComparator = new DefaultKeyComparator();

    protected BucketFileMeta addFile(long bucketKey, Slice startKey)
    {
      BucketFileMeta bfm = new BucketFileMeta();
      bfm.name = Long.toString(bucketKey) + '-' + this.fileSeq++;
      if (startKey.length != startKey.buffer.length) {
        // normalize key for serialization
        startKey = new Slice(startKey.toByteArray());
      }
      bfm.startKey = startKey;
      files.put(startKey, bfm);
      return bfm;
    }

    int fileSeq;
    long committedWid;
    final TreeMap<Slice, BucketFileMeta> files;
    HDHTWalManager.WalPosition recoveryStartWalPosition;
  }

  /**
   * Get meta data from cache or load it on first access
   *
   * @param bucketKey
   * @return The bucket meta.
   */
  private BucketMeta getMeta(long bucketKey)
  {
    BucketMeta bm = metaCache.get(bucketKey);
    if (bm == null) {
      bm = loadBucketMeta(bucketKey);
      metaCache.put(bucketKey, bm);
    }
    return bm;
  }

  private Bucket getBucket(long bucketKey) throws IOException
  {
    Bucket bucket = this.buckets.get(bucketKey);
    if (bucket == null) {
      LOG.debug("Opening bucket {}", bucketKey);
      bucket = new Bucket();
      bucket.bucketKey = bucketKey;
      this.buckets.put(bucketKey, bucket);
      BucketMeta bmeta = getMeta(bucketKey);
      WalMeta wmeta = getWalMeta(bucketKey);
      bucket.wal = new HDHTWalManager(this.store, bucketKey, wmeta.cpWalPosition);
      bucket.wal.setMaxWalFileSize(maxWalFileSize);

      LOG.debug("walStart {} walEnd {} windowId {} committedWid {} currentWid {}",
                bmeta.recoveryStartWalPosition, wmeta.cpWalPosition, wmeta.windowId, bmeta.committedWid, currentWindowId);

      // bmeta.componentLSN is data which is committed to disks.
      // wmeta.windowId windowId till which data is available in WAL.
      if (bmeta.committedWid < wmeta.windowId && wmeta.windowId != 0) {
        LOG.debug("Recovery for bucket {}", bucketKey);
        // Add tuples from recovery start till recovery end.
        bucket.wal.runRecovery(bucket.committedWriteCache, bmeta.recoveryStartWalPosition, wmeta.cpWalPosition);
        bucket.walPositions.put(wmeta.windowId, wmeta.cpWalPosition);
      }
    }
    return bucket;
  }

  public void delete(long bucketKey, Slice key) throws IOException
  {
    put(bucketKey, key, HDHT.WALReader.DELETED);
  }

  private WalMeta getWalMeta(long bucketKey)
  {
    WalMeta meta = walMeta.get(bucketKey);
    if (meta == null) {
      meta = new WalMeta();
      walMeta.put(bucketKey, meta);
    }
    return meta;
  }

  private static class Bucket
  {
    private long bucketKey;
    // keys that were modified and written to WAL, but not yet persisted, by checkpoint
    private HashMap<Slice, byte[]> writeCache = Maps.newHashMap();
    private final LinkedHashMap<Long, HashMap<Slice, byte[]>> checkpointedWriteCache = Maps.newLinkedHashMap();
    public HashMap<Long, HDHTWalManager.WalPosition> walPositions = Maps.newLinkedHashMap();
    private HashMap<Slice, byte[]> committedWriteCache = Maps.newHashMap();
    // keys that are being flushed to data files
    private HashMap<Slice, byte[]> frozenWriteCache = Maps.newHashMap();
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

  private void reset()
  {
    if (!setupCalled) {
      return;
    }
    if (!StringUtils.isBlank(smtpPassword)) {
      properties.setProperty("mail.smtp.auth", "true");
      properties.setProperty("mail.smtp.starttls.enable", "true");
      if (useSsl) {
        properties.setProperty("mail.smtp.socketFactory.port", String.valueOf(smtpPort));
        properties.setProperty("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        properties.setProperty("mail.smtp.socketFactory.fallback", "false");
      }

      auth = new Authenticator()
      {
        @Override
        protected PasswordAuthentication getPasswordAuthentication()
        {
          return new PasswordAuthentication(smtpUserName, smtpPassword);
        }

      };
    }

    properties.setProperty("mail.smtp.host", smtpHost);
    properties.setProperty("mail.smtp.port", String.valueOf(smtpPort));
    session = Session.getInstance(properties, auth);
    resetMessage();
  }

  private void resetMessage()
  {
    if (!setupCalled) {
      return;
    }
    try {
      message = new MimeMessage(session);
      message.setFrom(new InternetAddress(from));
      for (Map.Entry<String, String> entry: recipients.entrySet()) {
        com.datatorrent.lib.io.SMTP.SmtpOutputOperator.RecipientType type = com.datatorrent.lib.io.SMTP.SmtpOutputOperator.RecipientType.valueOf(entry.getKey().toUpperCase());
        Message.RecipientType recipientType;
        switch (type) {
          case TO:
            recipientType = Message.RecipientType.TO;
            break;
          case CC:
            recipientType = Message.RecipientType.CC;
            break;
          case BCC:
          default:
            recipientType = Message.RecipientType.BCC;
            break;
        }
        String[] addresses = entry.getValue().split(",");
        for (String address: addresses) {
          message.addRecipient(recipientType, new InternetAddress(address));
        }
      }
      message.setSubject(subject);
      LOG.debug("all recipients {}", Arrays.toString(message.getAllRecipients()));
    }
    catch (MessagingException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void checkpointed(long windowId)
  {
     for (Bucket bucket : this.buckets.values()) {
      if (!bucket.writeCache.isEmpty()) {
        bucket.checkpointedWriteCache.put(windowId, bucket.writeCache);
        bucket.walPositions.put(windowId, new HDHTWalManager.WalPosition(
            bucket.wal.getWalFileId(),
            bucket.wal.getWalSize()
        ));
        bucket.writeCache = Maps.newHashMap();
      }
    }
  }

  /**
   * Flush changes from write cache to disk. New data files will be written and meta data replaced atomically. The flush
   * frequency determines availability of changes to external readers.
   *
   * @throws IOException
   */
  private void writeDataFiles(Bucket bucket) throws IOException
  {
    LOG.debug("Writing data files in bucket {}", bucket.bucketKey);
    // copy meta data on write
    BucketMeta bucketMetaCopy = kryo.copy(getMeta(bucket.bucketKey));

    // bucket keys by file
    TreeMap<Slice, BucketFileMeta> bucketSeqStarts = bucketMetaCopy.files;
    Map<BucketFileMeta, Map<Slice, byte[]>> modifiedFiles = Maps.newHashMap();

    for (Map.Entry<Slice, byte[]> entry : bucket.frozenWriteCache.entrySet()) {
      // find file for key
      Map.Entry<Slice, BucketFileMeta> floorEntry = bucketSeqStarts.floorEntry(entry.getKey());
      BucketFileMeta floorFile;
      if (floorEntry != null) {
        floorFile = floorEntry.getValue();
      } else {
        floorEntry = bucketSeqStarts.firstEntry();
        if (floorEntry == null || floorEntry.getValue().name != null) {
          // no existing file or file with higher key
          floorFile = new BucketFileMeta();
        } else {
          // placeholder for new keys, move start key
          floorFile = floorEntry.getValue();
          bucketSeqStarts.remove(floorEntry.getKey());
        }
        floorFile.startKey = entry.getKey();
        if (floorFile.startKey.length != floorFile.startKey.buffer.length) {
          // normalize key for serialization
          floorFile.startKey = new Slice(floorFile.startKey.toByteArray());
        }
        bucketSeqStarts.put(floorFile.startKey, floorFile);
      }

      Map<Slice, byte[]> fileUpdates = modifiedFiles.get(floorFile);
      if (fileUpdates == null) {
        modifiedFiles.put(floorFile, fileUpdates = Maps.newHashMap());
      }
      fileUpdates.put(entry.getKey(), entry.getValue());
    }

    HashSet<String> filesToDelete = Sets.newHashSet();

    // write modified files
    for (Map.Entry<BucketFileMeta, Map<Slice, byte[]>> fileEntry : modifiedFiles.entrySet()) {
      BucketFileMeta fileMeta = fileEntry.getKey();
      TreeMap<Slice, byte[]> fileData = new TreeMap<Slice, byte[]>(getKeyComparator());

      if (fileMeta.name != null) {
        // load existing file
        HDSFileReader reader = store.getReader(bucket.bucketKey, fileMeta.name);
        reader.readFully(fileData);
        reader.close();
        filesToDelete.add(fileMeta.name);
      }

      // apply updates
      fileData.putAll(fileEntry.getValue());
      // new file
      writeFile(bucket, bucketMetaCopy, fileData);
    }

    // flush meta data for new files
    try {
      LOG.debug("Writing {} with {} file entries", FNAME_META, bucketMetaCopy.files.size());
      OutputStream os = store.getOutputStream(bucket.bucketKey, FNAME_META + ".new");
      Output output = new Output(os);
      bucketMetaCopy.committedWid = bucket.committedLSN;
      bucketMetaCopy.recoveryStartWalPosition = bucket.recoveryStartWalPosition;
      kryo.writeClassAndObject(output, bucketMetaCopy);
      output.close();
      os.close();
      store.rename(bucket.bucketKey, FNAME_META + ".new", FNAME_META);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write bucket meta data " + bucket.bucketKey, e);
    }

    bucket.frozenWriteCache.clear();
    // switch to new version
    this.metaCache.put(bucket.bucketKey, bucketMetaCopy);

    // delete old files
    for (String fileName : filesToDelete) {
      store.delete(bucket.bucketKey, fileName);
    }

    // cleanup WAL files which are not needed anymore.
    bucket.wal.cleanup(bucketMetaCopy.recoveryStartWalPosition.fileId);


  }

  /**
   * Write data to size based rolling files
   *
   * @param bucket
   * @param bucketMeta
   * @param data
   * @throws IOException
   */
  private void writeFile(Bucket bucket, BucketMeta bucketMeta, TreeMap<Slice, byte[]> data) throws IOException
  {
    long startTime = System.currentTimeMillis();

    HDSFileWriter fw = null;
    BucketFileMeta fileMeta = null;
    int keysWritten = 0;
    for (Map.Entry<Slice, byte[]> dataEntry : data.entrySet()) {
      if (fw == null) {
        // next file
        fileMeta = bucketMeta.addFile(bucket.bucketKey, dataEntry.getKey());
        LOG.debug("writing data file {} {}", bucket.bucketKey, fileMeta.name);
        fw = this.store.getWriter(bucket.bucketKey, fileMeta.name + ".tmp");
        keysWritten = 0;
      }

      if (dataEntry.getValue() == HDHT.WALReader.DELETED) {
        continue;
      }

      fw.append(dataEntry.getKey().toByteArray(), dataEntry.getValue());
      keysWritten++;
      if (fw.getBytesWritten() > this.maxFileSize) {
        // roll file
        fw.close();
        this.store.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
        LOG.debug("created data file {} {} with {} entries", bucket.bucketKey, fileMeta.name, keysWritten);
        fw = null;
        keysWritten = 0;
      }
    }

    if (fw != null) {
      fw.close();
      this.store.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
      LOG.debug("created data file {} {} with {} entries", bucket.bucketKey, fileMeta.name, keysWritten);
    }

  }


  @Override
  public void committed(long committedWindowId)
  {
    for (final Bucket bucket : this.buckets.values()) {
      for (Iterator<Map.Entry<Long, HashMap<Slice, byte[]>>> cpIter = bucket.checkpointedWriteCache.entrySet().iterator(); cpIter.hasNext();) {
        Map.Entry<Long, HashMap<Slice, byte[]>> checkpointEntry = cpIter.next();
        if (checkpointEntry.getKey() <= committedWindowId) {
          bucket.committedWriteCache.putAll(checkpointEntry.getValue());
          cpIter.remove();
        }
      }

      HDHTWalManager.WalPosition position = null;
      for (Iterator<Map.Entry<Long, HDHTWalManager.WalPosition>> wpIter = bucket.walPositions.entrySet().iterator(); wpIter.hasNext();) {
        Map.Entry<Long, HDHTWalManager.WalPosition> entry = wpIter.next();
        if (entry.getKey() <= committedWindowId) {
          position = entry.getValue();
          wpIter.remove();
        }
      }

      if ((bucket.committedWriteCache.size() > this.flushSize || currentWindowId - lastFlushWindowId > flushIntervalCount) && !bucket.committedWriteCache.isEmpty()) {
        // ensure previous flush completed
        if (bucket.frozenWriteCache.isEmpty()) {
          bucket.frozenWriteCache = bucket.committedWriteCache;

          bucket.committedLSN = committedWindowId;
          bucket.recoveryStartWalPosition = position;


          bucket.committedWriteCache = Maps.newHashMap();
          LOG.debug("Flushing data for bucket {} committedWid {}", bucket.bucketKey, bucket.committedLSN);
          Runnable flushRunnable = new Runnable() {
            @Override
            public void run()
            {
              try {
                writeDataFiles(bucket);
              } catch (Throwable e) {
                LOG.debug("Write error: {}", e.getMessage());
                writerError = e;
              }
            }
          };
          this.writeExecutor.execute(flushRunnable);
          lastFlushWindowId = committedWindowId;
        }
      }
    }

    // propagate writer exceptions
    if (writerError != null) {
      throw new RuntimeException("Error while flushing write cache.", this.writerError);
    }

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
    super.endWindow();
    for (Bucket bucket: this.buckets.values()) {
      try {
        if (bucket.wal != null) {
          bucket.wal.endWindow(currentWindowId);
          WalMeta walMeta = getWalMeta(bucket.bucketKey);
          walMeta.cpWalPosition = bucket.wal.getCurrentPosition();
          walMeta.windowId = currentWindowId;
        }
      }
      catch (IOException e) {
        throw new RuntimeException("Failed to flush WAL", e);
      }
    }

    // propagate writer exceptions
    if (writerError != null) {
      throw new RuntimeException("Error while flushing write cache.", this.writerError);
    }

  }

  @Override
  public void teardown()
  {
    for (Bucket bucket : this.buckets.values()) {
      IOUtils.closeQuietly(bucket.wal);
    }
    writeExecutor.shutdown();
    super.teardown();
  }

}
