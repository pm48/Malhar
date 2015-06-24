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
import java.util.logging.Level;
import org.junit.Test;

public class CouchbasePOJOTest
{
  private static final Logger logger = LoggerFactory.getLogger(CouchBaseInputOperatorTest.class);
  private static String APP_ID = "CouchBaseInputOperatorTest";
  private static String bucket = "default";
  private static String password = "";
  private static int OPERATOR_ID = 0;
  protected static ArrayList<URI> nodes = new ArrayList<URI>();
  protected static ArrayList<String> keyList;
  private static String uri = "node13.morado.com:8091";
  private static final String DESIGN_DOC_ID = "_design/CouchbaseTest";
  private static final String TEST_VIEW = "testView";

  @Test
  public void TestCouchBaseInputOperator()
  {
    CouchBaseWindowStore store = new CouchBaseWindowStore();
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
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    TestInputOperator inputOperator = new TestInputOperator();
    inputOperator.setStore(store);
    inputOperator.setServerURIString(uri);

    ArrayList<String> keylist = new ArrayList<String>();
     keylist.add("key1");
     keylist.add("key2");
     keylist.add("key3");
     keylist.add("key4");
    inputOperator.setKeys(keylist);
    inputOperator.setObjectClass("com.datatorrent.contrib.couchbase.TestPojoInput");
    inputOperator.insertEventsInTable(2);

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(context);
    inputOperator.createAndFetchViewQuery();
    inputOperator.setDesignDocumentName("dev_beer");
    inputOperator.setViewName("by_name");
    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

     int count = 0;
    for (Object o: sink.collectedTuples) {
      logger.debug("collected tuples are {}", sink.collectedTuples.size());
      count++;
      TestPojoInput object = (TestPojoInput)o;
      if (count == 1) {
        logger.debug("key is {} count 1", object.getKey());
       // Assert.assertEquals("name set in testpojo", "test1", object.getName());
        Assert.assertEquals(" set in testpojo", "123", object.getKey());
      }
      if (count == 2) {
        //logger.debug("name is {} count 2",object.get);
       // Assert.assertEquals("id set in testpojo", "321", object.getId().toString());
      }
    }
  }

  public static class TestInputOperator extends CouchBasePOJOInputOperator
  {

    private void insertEventsInTable(int numEvents)
    {
      logger.info("number of events is" + numEvents);
        TestPojoInput inputPojo = new TestPojoInput();
        TestPojoInput inputPojo2 = new TestPojoInput();
        inputPojo.setKey("Key1");
        inputPojo.setAge(23);
        TestPojoInput.Address address = new  TestPojoInput.Address();
        address.setCity("chandigarh");
        address.setHousenumber(34);
        inputPojo.setAddress(address);
        inputPojo2.setKey("Key2");
        inputPojo2.setAge(32);
        TestPojoInput.Address address2 = new  TestPojoInput.Address();
        address2.setCity("delhi");
        address2.setHousenumber(43);
        inputPojo2.setAddress(address2);
        try {
          store.client.set("Key1", inputPojo).get();
          store.client.set("Key2", inputPojo2).get();
        }
        catch (InterruptedException ex) {
          DTThrowable.rethrow(ex);
        }
        catch (ExecutionException ex) {
          DTThrowable.rethrow(ex);
        }

    }

    public void createAndFetchViewQuery()
    {
      DesignDocument designDoc = new DesignDocument("dev_beer");

      String viewName = "by_name";
      String mapFunction
              = "function (doc, meta) {\n"
              + //  "  if(doc.type && doc.type == \"beer\") {\n" +
              "    emit(meta.id, null);\n"
              + "  }\n"
              + "}";

      ViewDesign viewDesign = new ViewDesign(viewName, mapFunction);
      designDoc.getViews().add(viewDesign);
      while (store.client.createDesignDoc(designDoc) != true) {
        try {
          Thread.sleep(1000);
          //return new ViewQuery().designDocId(DESIGN_DOC_ID).viewName(TEST_VIEW).includeDocs(true);
        }
        catch (InterruptedException ex) {
          java.util.logging.Logger.getLogger(CouchbasePOJOTest.class.getName()).log(Level.SEVERE, null, ex);
        }
      }
    }

  }

}
