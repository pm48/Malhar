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

import java.net.URI;
import java.util.*;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.google.common.collect.Lists;

import org.couchbase.mock.Bucket;
import org.couchbase.mock.Bucket.BucketType;
import org.couchbase.mock.BucketConfiguration;
import org.couchbase.mock.CouchbaseMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertEquals;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.partitioner.StatelessPartitionerTest.PartitioningContextImpl;
import com.datatorrent.lib.testbench.CollectorTestSink;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Partitioner.Partition;

import com.datatorrent.common.util.DTThrowable;

public class CouchBaseInputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(CouchBaseInputOperatorTest.class);
  private static String APP_ID = "CouchBaseInputOperatorTest";
  private static String bucket = "default";
  private static String password = "";
  private static int OPERATOR_ID = 0;
  protected static ArrayList<URI> nodes = new ArrayList<URI>();
  protected static ArrayList<String> keyList;
  private CouchbaseMock mockCouchbase1 = null;
  private CouchbaseMock mockCouchbase2 = null;
  private TestInputOperator inputOperator = null;
  protected static CouchbaseClient client = null;
  private BucketConfiguration bucketConfiguration = new BucketConfiguration();
  private int numNodes = 2;
  private int numVBuckets = 8;
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
    }
    if (mockCouchbase1 != null) {
      mockCouchbase1.stop();
      mockCouchbase1 = null;
    }
    if (mockCouchbase2 != null) {
      mockCouchbase2.stop();
      mockCouchbase2 = null;
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
    ArrayList<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
    configList.add(bucketConfiguration);
    mockCouchbase1 = new CouchbaseMock(0, configList);
    mockCouchbase1.start();
  }

  @Test
  public void TestCouchBaseInputOperator() throws InterruptedException, Exception
  {
    createMock("default", "");
    List<URI> uriList = new ArrayList<URI>();
    int port0 = mockCouchbase1.getHttpPort();
    logger.debug("port is {}", port0);
    mockCouchbase2 = new CouchbaseMock("localhost", 0, numNodes, numVBuckets);

    mockCouchbase2.start();
    mockCouchbase2.waitForStartup();
    int port1 = mockCouchbase2.getHttpPort();
    logger.debug("port is {}", port1);
    uriList.add(new URI("http", null, "localhost", port0, "/pools", "", ""));
    connectionFactory = cfb.buildCouchbaseConnection(uriList, bucketConfiguration.name, bucketConfiguration.password);
    client = new CouchbaseClient(connectionFactory);

    CouchBaseStore store = new CouchBaseWindowStore();
    keyList = new ArrayList<String>();
    store.setBucket(bucketConfiguration.name);
    store.setPasswordConfig(password);
    store.setPassword(bucketConfiguration.password);
    store.setUriString("localhost:" + port0 + "," + "localhost:" + port1);

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
    Assert.assertEquals(2, newPartitions.size());
    for (Partition<AbstractCouchBaseInputOperator<String>> p: newPartitions) {
      Assert.assertNotSame(inputOperator, p.getPartitionedInstance());
    }
    //Collect all operators in a list
    List<AbstractCouchBaseInputOperator<String>> opers = Lists.newArrayList();
    for (Partition<AbstractCouchBaseInputOperator<String>> p: newPartitions) {
      TestInputOperator oi = (TestInputOperator)p.getPartitionedInstance();
      oi.setServerURIString("localhost:" + port0);
      oi.setStore(store);
      oi.setup(null);
      oi.outputPort.setSink(sink);
      opers.add(oi);
      port0 = port1;

    }

    sink.clear();
    int wid = 0;
    for (int i = 0; i < 10; i++) {
      for (AbstractCouchBaseInputOperator<String> o: opers) {
        o.beginWindow(wid);
        o.emitTuples();
        o.endWindow();
      }
      wid++;
    }
    Assert.assertEquals("Tuples read should be same ", 10, sink.collectedTuples.size());

    teardown();
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
      client.shutdown();
    }

  }

}
