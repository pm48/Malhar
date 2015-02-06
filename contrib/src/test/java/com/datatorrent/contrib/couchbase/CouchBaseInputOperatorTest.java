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
package com.datatorrent.contrib.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.Partitioner.PartitioningContext;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.partitioner.StatelessPartitionerTest.PartitioningContextImpl;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.Bucket.BucketType;
import org.couchbase.mock.BucketConfiguration;
import org.couchbase.mock.CouchbaseBucket;
import org.couchbase.mock.CouchbaseMock;

import org.junit.After;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class CouchBaseInputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(CouchBaseInputOperatorTest.class);
  private static String APP_ID = "CouchBaseInputOperatorTest";
  private static String bucket = "default";
  private static String password = "";
  private static int OPERATOR_ID = 0;
  protected static ArrayList<URI> nodes = new ArrayList<URI>();
  protected static ArrayList<String> keyList;
  private CouchbaseMock mockCouchbase = null;
  private TestInputOperator inputOperator = null;
  protected static CouchbaseClient client = null;
  private TestCouchbaseBucketConfig bucketConfiguration = new TestCouchbaseBucketConfig();
  private int numNodes = 4;
  private int numVBuckets = 16;
  protected final CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();

  protected CouchbaseConnectionFactory connectionFactory;

  @Test
  public void testDefaults() throws Exception
  {
    CouchbaseMock mock = new CouchbaseMock(null, 8091, numNodes, numVBuckets);
    Map<String, Bucket> buckets = mock.getBuckets();
    assertEquals(1, buckets.size());
    assert (buckets.containsKey("default"));
    assertEquals("", buckets.get("default").getPassword());
    assertEquals(Bucket.BucketType.COUCHBASE, buckets.get("default").getType());
  }

  @Test
  public void testPasswords() throws Exception
  {
    CouchbaseMock mock = new CouchbaseMock(null, 8091, numNodes, numVBuckets, "xxx:,yyy:pass,zzz");
    Map<String, Bucket> buckets = mock.getBuckets();
    assertEquals(3, buckets.size());
    assert (buckets.containsKey("xxx"));
    assert (buckets.containsKey("yyy"));
    assert (buckets.containsKey("zzz"));
    assertEquals("", buckets.get("xxx").getPassword());
    assertEquals("", buckets.get("zzz").getPassword());
    assertEquals("pass", buckets.get("yyy").getPassword());
  }

  @Test
  public void testTypes() throws Exception
  {
    CouchbaseMock mock = new CouchbaseMock(null, 8091, numNodes, numVBuckets, "xxx::,yyy::memcache,zzz,kkk::couchbase,aaa::unknown");
    Map<String, Bucket> buckets = mock.getBuckets();
    assertEquals(5, buckets.size());
    assert (buckets.containsKey("xxx"));
    assert (buckets.containsKey("yyy"));
    assert (buckets.containsKey("zzz"));
    assert (buckets.containsKey("kkk"));
    assert (buckets.containsKey("aaa"));
    assertEquals(Bucket.BucketType.COUCHBASE, buckets.get("xxx").getType());
    assertEquals(Bucket.BucketType.MEMCACHED, buckets.get("yyy").getType());
    assertEquals(Bucket.BucketType.COUCHBASE, buckets.get("zzz").getType());
    assertEquals(Bucket.BucketType.COUCHBASE, buckets.get("kkk").getType());
    assertEquals(Bucket.BucketType.COUCHBASE, buckets.get("aaa").getType());
  }

  @Test
  public void testMixed() throws Exception
  {
    CouchbaseMock mock = new CouchbaseMock(null, 8091, numNodes, numVBuckets, "xxx:pass:memcache,yyy:secret:couchbase");
    Map<String, Bucket> buckets = mock.getBuckets();
    assertEquals(2, buckets.size());
    assert (buckets.containsKey("xxx"));
    assert (buckets.containsKey("yyy"));
    assertEquals(Bucket.BucketType.MEMCACHED, buckets.get("xxx").getType());
    assertEquals(Bucket.BucketType.COUCHBASE, buckets.get("yyy").getType());
    assertEquals("pass", buckets.get("xxx").getPassword());
    assertEquals("secret", buckets.get("yyy").getPassword());
  }

  @After
  public void teardown() throws Exception
  {
     if (inputOperator != null) {
      inputOperator.teardown();
      client.flush();
    }
    if (mockCouchbase != null) {
      mockCouchbase.stop();
      mockCouchbase = null;
    }
  }

  protected void createMock(String name, String password) throws Exception
  {

    bucketConfiguration.numNodes = numNodes;
    bucketConfiguration.numReplicas = 3;
    bucketConfiguration.name = name;
    bucketConfiguration.type = BucketType.COUCHBASE;
    bucketConfiguration.password = password;
    bucketConfiguration.hostname = "localhost";
    bucketConfiguration.port = 1100;
    List<URL> uriList = new ArrayList<URL>();
    uriList.add(new URL("http", "localhost", 1100, "/pools"));

    bucketConfiguration.couchServers = uriList;
    bucketConfiguration.couchServerCount = 10;

    ArrayList<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
    configList.add(bucketConfiguration);
    mockCouchbase = new CouchbaseMock("localhost", 0, numNodes, 1100, numVBuckets);
    mockCouchbase.start();
    mockCouchbase.waitForStartup();
  }

  public  class TestCouchbaseBucketConfig extends BucketConfiguration{
    public List<URL> couchServers;
    public int couchServerCount;

    public int getCouchServerCount()
    {
      return couchServerCount;
    }

    public void setCouchServerCount(int couchServerCount)
    {
      this.couchServerCount = couchServerCount;
    }

    public List<URL> getCouchServers()
    {
      return couchServers;
    }

    public void setCouchServers(List<URL> couchServers)
    {
      this.couchServers = couchServers;
    }

  }

  @Test
  public void TestCouchBaseInputOperator() throws InterruptedException, Exception
  {
    createMock("default", "");
    List<URI> uriList = new ArrayList<URI>();
    int port = mockCouchbase.getHttpPort();
    //  int port2 = mockCouchbase2.getHttpPort();
    uriList.add(new URI("http", null, "localhost", port, "/pools", "", ""));
    //  uriList.add(new URI("http", null, "localhost", port2, "/pools", "", ""));
    connectionFactory = cfb.buildCouchbaseConnection(uriList, bucketConfiguration.name, bucketConfiguration.password);
    client = new CouchbaseClient(connectionFactory);


    Bucket bucket = mockCouchbase.getBuckets().get(bucketConfiguration.name);
    //logger.info("bucket is {}",bucket.toString());
   // logger.info("bucket urls are",bucket.getServers());
    CouchBaseStore store = new CouchBaseStore();
    keyList = new ArrayList<String>();
    store.setBucket(bucketConfiguration.name);
    store.setPassword(bucketConfiguration.password);
    store.setUriString("localhost:" + port);
    //store.setServerURIString("localhost:"+ port);
   // logger.info("couch servers are {}",cfb.getVBucketConfig().getCouchServers());
   // logger.info(" servers are {}",cfb.getVBucketConfig().getServers());

   // couchbaseBucket.getCouchServers();
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    inputOperator = new TestInputOperator();
    inputOperator.setStore(store);
    inputOperator.insertEventsInTable(10);


    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);
    List<Partition<AbstractCouchBaseInputOperator<String>>> partitions = Lists.newArrayList();
    Collection<Partition<AbstractCouchBaseInputOperator<String>>> newPartitions = inputOperator.definePartitions(partitions, new PartitioningContextImpl(null, 0));
    Assert.assertEquals(5, newPartitions.size());
    inputOperator.setup(context);
    inputOperator.beginWindow(0);

    logger.info("couchservers are {}",inputOperator.conf.getCouchServers());
    logger.info("couchservers are {}",inputOperator.conf.getServers());

    inputOperator.setServerIndex(inputOperator.conf.getMaster(inputOperator.conf.getVbucketByKey("Key10")));
    inputOperator.emitTuples();
    inputOperator.endWindow();
    inputOperator.teardown();
    Assert.assertEquals("tuples in couchbase", 1, sink.collectedTuples.size());
    teardown();
  }

  public void TestCouchBaseInputOperatorWithPartitions() throws InterruptedException, Exception
  {
    createMock("default", "");
    List<URI> uriList = new ArrayList<URI>();
    int port1 = mockCouchbase.getHttpPort();
    // int port2 = mockCouchbase2.getHttpPort();
    uriList.add(new URI("http", null, "localhost", port1, "/pools", "", ""));
    // uriList.add(new URI("http", null, "localhost", port2, "/pools", "", ""));
    connectionFactory = cfb.buildCouchbaseConnection(uriList, bucketConfiguration.name, bucketConfiguration.password);
    client = new CouchbaseClient(connectionFactory);
    CouchBaseWindowStore store = new CouchBaseWindowStore();
    keyList = new ArrayList<String>();
    store.setBucket(bucketConfiguration.name);
    store.setPassword(bucketConfiguration.password);
    store.setUriString("localhost:" + port1);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    List<Partition<AbstractCouchBaseInputOperator<String>>> partitions = Lists.newArrayList();
    inputOperator = new TestInputOperator();
    inputOperator.setStore(store);
    inputOperator.insertEventsInTable(10);
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    partitions.add(new DefaultPartition<AbstractCouchBaseInputOperator<String>>(inputOperator));
    /*Collection<Partition<AbstractCouchBaseInputOperator<String>>> newPartitions = inputOperator.definePartitions(partitions);
//    Assert.assertEquals(2, newPartitions.size());
    for (Partition<AbstractCouchBaseInputOperator<String>> p: newPartitions) {
      Assert.assertNotSame(inputOperator, p.getPartitionedInstance());
    }
    /* Collect all operators in a list
    List<AbstractCouchBaseInputOperator<String>> opers = Lists.newArrayList();
    for (Partition<AbstractCouchBaseInputOperator<String>> p: newPartitions) {
      TestInputOperator oi = (TestInputOperator)p.getPartitionedInstance();
      oi.setStore(store);
      oi.setup(null);
      oi.outputPort.setSink(sink);
      opers.add(oi);
    } */

    sink.clear();
    int wid = 0;
    for (int i = 0; i < 10; i++) {
     /* for (AbstractCouchBaseInputOperator<String> o: opers) {
        o.beginWindow(wid);
        o.emitTuples();
        o.endWindow();
      }*/
      wid++;
    }
    Assert.assertEquals("Tuples read should be same ", 100, sink.collectedTuples.size());
  }

  public static class TestInputOperator extends AbstractCouchBaseInputOperator<String>
  {

    @SuppressWarnings("unchecked")
    @Override
    public String getTuple(Object entry)
    {
      String tuple = entry.toString();
      logger.debug("returned tuple is {}", tuple);
      return tuple;
    }

    @Override
    public ArrayList<String> getKeys()
    {
      return keyList;
    }

    public void insertEventsInTable(int numEvents)
    {
      String key = null;
      Integer value = null;
      logger.debug("number of events is {}", numEvents);
      for (int i = 0; i < numEvents; i++) {
        key = String.valueOf("Key" + i * 10);
        keyList.add(key);
        value = i * 100;
        try {
          client.set(key, value).get();
        }
        catch (InterruptedException ex) {
          DTThrowable.rethrow(ex);
        }
        catch (ExecutionException ex) {
          DTThrowable.rethrow(ex);
        }
      }

    }

  }

}
