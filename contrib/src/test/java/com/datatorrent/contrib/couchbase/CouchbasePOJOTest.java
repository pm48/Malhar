/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.couchbase;

import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.ViewDesign;
import java.io.IOException;
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

import com.datatorrent.common.util.DTThrowable;
import org.junit.Test;

public class CouchbasePOJOTest
{
  private static final Logger logger = LoggerFactory.getLogger(CouchBaseInputOperatorTest.class);
  private static final String APP_ID = "CouchBaseInputOperatorTest";
  private static final String bucket = "default";
  private static final String password = "";
  private static final int OPERATOR_ID = 0;
  protected static ArrayList<URI> nodes = new ArrayList<URI>();
  protected static ArrayList<String> keyList;
  private static final String uri = "node13.morado.com:8091";
  private static final String DESIGN_DOC_ID1 = "dev_test1";
  private static final String DESIGN_DOC_ID2 = "dev_test1";
  private static final String TEST_VIEW1 = "testView1";
  private static final String TEST_VIEW2 = "testView2";


  @Test
  public void TestCouchBaseInputOperator()
  {
    CouchBaseWindowStore store = new CouchBaseWindowStore();
    System.setProperty("viewmode", "development");
    keyList = new ArrayList<String>();
    store.setBucket(bucket);
    store.setPassword(password);
    store.setUriString(uri);
    try {
      store.connect();
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    store.getInstance().flush();
    try {
      Thread.sleep(1000);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);
    ArrayList<String> columns = new ArrayList<String>();
    columns.add("key");
    columns.add("name");
    columns.add("map");
    columns.add("age");
    TestInputOperator inputOperator = new TestInputOperator();
    inputOperator.setStore(store);
    inputOperator.setServerURIString(uri);
    inputOperator.setColumns(columns);
    inputOperator.setOutputClass("com.datatorrent.contrib.couchbase.TestComplexPojoInput");
    inputOperator.insertEventsInTable(2);

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(context);
    inputOperator.createAndFetchViewQuery1();
    inputOperator.setDesignDocumentName(DESIGN_DOC_ID1);
    inputOperator.setViewName(TEST_VIEW1);
    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

     int count = 0;
    for (Object o: sink.collectedTuples) {
      logger.debug("collected tuples are {}", sink.collectedTuples.size());
      count++;
      TestComplexPojoInput object = (TestComplexPojoInput)o;
      if (count == 1) {
        Assert.assertEquals("key is", "Key2", object.getKey());
        Assert.assertEquals("name set in testpojo", "test", object.getName());
        Assert.assertEquals("map in testpojo", "\"test\":12345", object.getMap());
        Assert.assertEquals("age in testpojo", 23, object.getAge());
      }
      if (count == 2) {
       Assert.assertEquals("map in testpojo", "\"test2\":12345", object.getMap());
       Assert.assertEquals("age in testpojo", 12, object.getAge());
       Assert.assertEquals("key is", "Key3", object.getKey());
       Assert.assertEquals("name set in testpojo", "test1", object.getName());
      }
    }
    sink.clear();
    inputOperator.teardown();

    inputOperator.setOutputClass("com.datatorrent.contrib.couchbase.TestSimplePojoInput");
    inputOperator.setup(context);
    inputOperator.createAndFetchViewQuery2();
    inputOperator.setDesignDocumentName(DESIGN_DOC_ID2);
    inputOperator.setViewName(TEST_VIEW2);
    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

     Assert.assertEquals("name set in testpojo", 431, (TestSimplePojoInput)sink.collectedTuples.get(0));

    store.client.deleteDesignDoc(DESIGN_DOC_ID1);
    store.client.deleteDesignDoc(DESIGN_DOC_ID2);
  }



  public static class TestInputOperator extends CouchBasePOJOInputOperator
  {

    private void insertEventsInTable(int numEvents)
    {
      logger.info("number of events is" + numEvents);
        try {
          store.client.set("Key1", 431);
          store.client.set("Key2", "{\"name\":\"test\",\"map\":{\"test\":12345},\"age\":23}").get();
          store.client.set("Key3", "{\"name\":\"test1\",\"map\":{\"test2\":12345},\"age\":12}").get();
        }
        catch (InterruptedException ex) {
          DTThrowable.rethrow(ex);
        }
        catch (ExecutionException ex) {
          DTThrowable.rethrow(ex);
        }

    }

    public void createAndFetchViewQuery1()
    {
      DesignDocument designDoc = new DesignDocument(DESIGN_DOC_ID1);
      String viewName = TEST_VIEW1;
      String mapFunction =
            "function (doc, meta) {\n" +
            "  if( meta.type == \"json\") {\n" +
            "    emit(doc.key,[doc.name, doc.test, doc.map, doc.phone]);\n" +
            "  }\n" +
            " }";

      ViewDesign viewDesign = new ViewDesign(viewName, mapFunction);
      designDoc.getViews().add(viewDesign);
      store.client.createDesignDoc(designDoc);
    }

    public void createAndFetchViewQuery2()
    {
      DesignDocument designDoc = new DesignDocument(DESIGN_DOC_ID2);
      String viewName = TEST_VIEW2;
      String mapFunction =
            "function (doc, meta) {\n" +
            "  if( meta.type == \"json\") {\n" +
            "    emit(doc.key,doc);\n" +
            "  }\n" +
            " }";

      ViewDesign viewDesign = new ViewDesign(viewName, mapFunction);
      designDoc.getViews().add(viewDesign);
      store.client.createDesignDoc(designDoc);
    }

  }

}
