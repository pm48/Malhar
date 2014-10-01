/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.testbench.EventClassifier;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author prerna
 */
public class EventClassifierApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    HashMapOperator hmapOper = dag.addOperator("hmap", new HashMapOperator());
    dag.getMeta(hmapOper).getMeta(hmapOper.hmap_data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    EventClassifier eventInput = dag.addOperator("eventInput", new EventClassifier());
    dag.getMeta(eventInput).getMeta(eventInput.data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("eventtest1", hmapOper.hmap_data, eventInput.event).setLocality(locality);
    DevNull<HashMap<String,Double>> dev = dag.addOperator("dev", new DevNull());
    dag.addStream("eventtest2", eventInput.data, dev.data).setLocality(locality);

  }

}
