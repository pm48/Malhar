/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.couchbase;


import com.couchbase.client.CouchbaseClient;
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
  private static String APP_ID = "CouchBaseInputOperatorTest";
  private static String bucket = "default";
  private static String password = "";
  private static int OPERATOR_ID = 0;
  protected static ArrayList<URI> nodes = new ArrayList<URI>();
  protected static ArrayList<String> keyList;
  private static String uri = "node13.morado.com:8091,node14.morado.com:8091";
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
    inputOperator.insertEventsInTable(100);

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(context);
    inputOperator.createAndFetchViewQuery();
    inputOperator.setDesignDocumentName(DESIGN_DOC_ID);
    inputOperator.setViewName(TEST_VIEW);
    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    Assert.assertEquals("tuples in couchbase", 100, sink.collectedTuples.size());
  }

  public static class TestInputOperator extends CouchBasePOJOInputOperator
  {

    private void insertEventsInTable(int numEvents)
    {
      String key = null;
      Integer value = null;
      logger.info("number of events is" + numEvents);
      for (int i = 0; i < numEvents; i++) {
        key = String.valueOf("Key" + i * 10);
        keyList.add(key);
        value = i * 100;
        try {
          store.client.set(key, value).get();
        }
        catch (InterruptedException ex) {
          DTThrowable.rethrow(ex);
        }
        catch (ExecutionException ex) {
          DTThrowable.rethrow(ex);
        }
      }
    }

     public void createAndFetchViewQuery()
    {
      DesignDocument designDoc = new DesignDocument(DESIGN_DOC_ID);

      String mapFunction ="function(doc, meta)\n" +
"{\n" +
"  emit(doc.name, [doc.city, doc.salary]);\n" +
"}";
      
 logger.debug("mapfunction is {}",mapFunction);
    ViewDesign viewDesign = new ViewDesign(TEST_VIEW, mapFunction);

        designDoc.getViews().add(viewDesign);

        store.client.createDesignDoc(designDoc);
        //return new ViewQuery().designDocId(DESIGN_DOC_ID).viewName(TEST_VIEW).includeDocs(true);
      }


  }

}
