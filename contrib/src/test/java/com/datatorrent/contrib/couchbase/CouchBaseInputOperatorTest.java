/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.ViewDesign;
import com.google.common.collect.Lists;

import org.couchbase.mock.Bucket.BucketType;
import org.couchbase.mock.BucketConfiguration;
import org.couchbase.mock.CouchbaseMock;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.partitioner.StatelessPartitionerTest.PartitioningContextImpl;
import com.datatorrent.lib.testbench.CollectorTestSink;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Partitioner.Partition;

import com.datatorrent.common.util.DTThrowable;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.ViewQuery;

public class CouchBaseInputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(CouchBaseInputOperatorTest.class);
  private static final String APP_ID = "CouchBaseInputOperatorTest";
  private static final String password = "";
  private static final int OPERATOR_ID = 0;
  protected static ArrayList<String> keyList;
  private TestInputOperator inputOperator = null;
  protected static CouchbaseClient client = null;
  private final int numNodes = 2;
  private final int numReplicas = 3;
  private static final String DESIGN_DOC_ID = "_design/CouchbaseTest";
  private static final String TEST_VIEW = "testView";

  protected CouchbaseConnectionFactory connectionFactory;

  protected CouchbaseMock createMock(String name, String password, BucketConfiguration bucketConfiguration) throws Exception
  {
    bucketConfiguration.numNodes = numNodes;
    bucketConfiguration.numReplicas = numReplicas;
    bucketConfiguration.name = name;
    bucketConfiguration.type = BucketType.COUCHBASE;
    bucketConfiguration.password = password;
    bucketConfiguration.hostname = "localhost";
    ArrayList<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
    configList.add(bucketConfiguration);
    CouchbaseMock mockCouchbase = new CouchbaseMock(0, configList);
    return mockCouchbase;
  }

  @Test
  public void TestCouchBaseInputOperator() throws InterruptedException, Exception
  {
    BucketConfiguration bucketConfiguration = new BucketConfiguration();
    CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
    CouchbaseMock mockCouchbase1 = createMock("default", "", bucketConfiguration);
    CouchbaseMock mockCouchbase2 = createMock("default", "", bucketConfiguration);
    mockCouchbase1.start();
    mockCouchbase1.waitForStartup();
    List<URI> uriList = new ArrayList<URI>();
    int port1 = mockCouchbase1.getHttpPort();
    logger.debug("port is {}", port1);
    mockCouchbase2.start();
    mockCouchbase2.waitForStartup();
    int port2 = mockCouchbase2.getHttpPort();
    logger.debug("port is {}", port2);
    uriList.add(new URI("http", null, "localhost", port1, "/pools", "", ""));
    connectionFactory = cfb.buildCouchbaseConnection(uriList, bucketConfiguration.name, bucketConfiguration.password);
    client = new CouchbaseClient(connectionFactory);

    CouchBaseStore store = new CouchBaseStore();
    keyList = new ArrayList<String>();
    store.setBucket(bucketConfiguration.name);
    store.setPasswordConfig(password);
    store.setPassword(bucketConfiguration.password);
    store.setUriString("localhost:" + port1 + "," + "localhost:" + port1);

    // couchbaseBucket.getCouchServers();
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);

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
      oi.setServerURIString("localhost:" + port1);
      oi.setStore(store);
      oi.setup(null);
      oi.outputPort.setSink(sink);
      opers.add(oi);
      port1 = port2;

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
    for (AbstractCouchBaseInputOperator<String> o: opers) {
      o.teardown();
    }

    mockCouchbase1.stop();

    mockCouchbase2.stop();

  }

  @Test
  public void TestPOJO() throws InterruptedException, Exception
  {

    BucketConfiguration bucketConfiguration = new BucketConfiguration();
    CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
    CouchbaseMock mockCouchbase1 = createMock("default", "", bucketConfiguration);
    CouchbaseMock mockCouchbase2 = createMock("default", "", bucketConfiguration);
    mockCouchbase1.start();
    mockCouchbase1.waitForStartup();
    mockCouchbase2.start();
    mockCouchbase2.waitForStartup();
    int port2 = mockCouchbase2.getHttpPort();
    logger.debug("port is {}", port2);
    List<URI> uriList = new ArrayList<URI>();
    int port1 = mockCouchbase1.getHttpPort();
    logger.debug("port is {}", port1);
    uriList.add(new URI("http", null, "localhost", port1, "/pools", "", ""));
    connectionFactory = cfb.buildCouchbaseConnection(uriList, bucketConfiguration.name, bucketConfiguration.password);
    client = new CouchbaseClient(connectionFactory);

    CouchBaseStore store = new CouchBaseStore();

    store.setBucket(bucketConfiguration.name);
    store.setPasswordConfig(password);
    store.setPassword(bucketConfiguration.password);
    store.setUriString("localhost:" + port1);
    System.setProperty("viewmode", "development");

    // couchbaseBucket.getCouchServers();
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    TestInputPOJOOperator inputPOJOOperator = new TestInputPOJOOperator();
    inputPOJOOperator.setStore(store);
    //inputPOJOOperator.insertEventInTable();
    ArrayList<String> expressions = new ArrayList<String>();
    expressions.add("name");
    expressions.add("id");
    inputPOJOOperator.setExpressionForValue(expressions);
     ArrayList<String> keylist = new ArrayList<String>();
     keylist.add("key1");
     keylist.add("key2");
     keylist.add("key3");
     keylist.add("key4");
    inputPOJOOperator.setKeys(keylist);
    inputPOJOOperator.setObjectClass("com.datatorrent.contrib.couchbase.TestPojoInput");
    inputPOJOOperator.setViewName(TEST_VIEW);
    inputPOJOOperator.insertEventInTable();
    inputPOJOOperator.setDesignDocumentName(DESIGN_DOC_ID);
   // inputPOJOOperator.setQuery(query.allDocs());
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputPOJOOperator.setServerURIString("localhost:" + port1);
    inputPOJOOperator.outputPort.setSink(sink);
    inputPOJOOperator.setup(null);
         inputPOJOOperator.createAndFetchViewQuery(store.getInstance());

     inputPOJOOperator.beginWindow(0);
      inputPOJOOperator.emitTuples();
      inputPOJOOperator.endWindow();
    /*List<Partition<AbstractCouchBaseInputOperator<Object>>> partitions = Lists.newArrayList();
    //Collection<Partition<AbstractCouchBaseInputOperator<Object>>> newPartitions = inputPOJOOperator.definePartitions(partitions, new PartitioningContextImpl(null, 0));
    List<AbstractCouchBaseInputOperator<Object>> opers = Lists.newArrayList();
   /* for (Partition<AbstractCouchBaseInputOperator<Object>> p: newPartitions) {
      TestInputPOJOOperator oi = (TestInputPOJOOperator)p.getPartitionedInstance();
      oi.setServerURIString("localhost:" + port1);
      oi.setStore(store);
      // oi.setKeys(keylist);
      oi.setExpressionForValue(expressions);
      oi.setup(null);
      oi.insertEventInTable();

      //  oi.outputPort.setSink(sink);
      opers.add(oi);
      port1 = port2;

    }

     sink.clear();
    int wid = 0;
    for (AbstractCouchBaseInputOperator<Object> o: opers) {
      o.beginWindow(wid);
      o.emitTuples();
      o.endWindow();
    }*/

    int count = 0;
    for (Object o: sink.collectedTuples) {
      logger.debug("collected tuples are {}", sink.collectedTuples.size());
      count++;
      TestPojoInput object = (TestPojoInput)o;
      if (count == 1) {
        logger.debug("name is {} count 1", object.getName());
        Assert.assertEquals("name set in testpojo", "test1", object.getName());
        Assert.assertEquals("id set in testpojo", "123", object.getId().toString());
      }
      if (count == 2) {
        logger.debug("name is {} count 2", object.getName());
        Assert.assertEquals("id set in testpojo", "321", object.getId().toString());
      }
    }

  /*  for (AbstractCouchBaseInputOperator<Object> o: opers) {
      o.teardown();
    }*/

    mockCouchbase1.stop();

    mockCouchbase2.stop();

  }

  public void teardown()
  {
    client.shutdown();
    client = null;
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
      String key;
      Integer value;
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
      client = null;
    }

  }

  public static class TestInputPOJOOperator extends CouchBasePOJOInputOperator
  {

    public void insertEventInTable()
    {
      try {
        client.set("Key1", "test1").get();
        client.set("Key2", "test2").get();
        client.set("Key3", 123).get();
        client.set("Key4", 321).get();
      }
      catch (InterruptedException ex) {
        DTThrowable.rethrow(ex);
      }
      catch (ExecutionException ex) {
        DTThrowable.rethrow(ex);
      }
    }

    public void createAndFetchViewQuery(CouchbaseClient client)
    {
      DesignDocument designDoc = new DesignDocument(DESIGN_DOC_ID);

      String mapFunction ="\"function(doc){emit(doc._id, null)}\"";
 logger.debug("mapfunction is {}",mapFunction);
    ViewDesign viewDesign = new ViewDesign(TEST_VIEW, mapFunction);

        designDoc.getViews().add(viewDesign);

        client.createDesignDoc(designDoc);
        //return new ViewQuery().designDocId(DESIGN_DOC_ID).viewName(TEST_VIEW).includeDocs(true);
      }

    }

  }
