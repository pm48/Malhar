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
import com.datatorrent.lib.testbench.SeedEventGenerator;
import com.datatorrent.lib.util.KeyValPair;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author prerna
 */
public class SeedEventGeneratorApp implements StreamingApplication
{
  public static final int QUEUE_CAPACITY = 16 * 1024;
  private final Locality locality = null;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    SeedEventGenerator seedEvent = dag.addOperator("seedEvent", new SeedEventGenerator());
    seedEvent.addKeyData("x", 0, 9);
    seedEvent.addKeyData("y", 0, 9);
    seedEvent.addKeyData("gender", 0, 1);
    seedEvent.addKeyData("age", 10, 19);
    DevNull<HashMap<String, String>> devString = dag.addOperator("devString", new DevNull<HashMap<String, String>>());
    DevNull<HashMap<String, ArrayList<KeyValPair>>> devKeyVal = dag.addOperator("devKeyVal", new DevNull());
    DevNull<HashMap<String, String>> devVal = dag.addOperator("devVal", new DevNull<HashMap<String, String>>());
    DevNull<HashMap<String, ArrayList<Integer>>> devList = dag.addOperator("devList", new DevNull());

    dag.getMeta(seedEvent).getMeta(seedEvent.string_data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("SeedEventGeneratorString", seedEvent.string_data, devString.data).setLocality(locality);

    dag.getMeta(seedEvent).getMeta(seedEvent.keyvalpair_list).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("SeedEventGeneratorKeyVal", seedEvent.keyvalpair_list, devKeyVal.data).setLocality(locality);

    dag.getMeta(seedEvent).getMeta(seedEvent.val_data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("SeedEventGeneratorVal", seedEvent.val_data, devVal.data).setLocality(locality);

    dag.getMeta(seedEvent).getMeta(seedEvent.val_list).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("SeedEventGeneratorValList", seedEvent.val_list, devList.data).setLocality(locality);

  }

}
