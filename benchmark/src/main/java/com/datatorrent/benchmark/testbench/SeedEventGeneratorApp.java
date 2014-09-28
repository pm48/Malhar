/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.benchmark.WordCountOperator;
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
      WordCountOperator<HashMap<String,String>> counterString = dag.addOperator("counterString", new WordCountOperator<HashMap<String,String>>());
      dag.getMeta(counterString).getMeta(counterString.input).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
      dag.getMeta(seedEvent).getMeta(seedEvent.string_data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
      dag.addStream("SeedEventGeneratorString", seedEvent.string_data ,  counterString.input).setLocality(locality);

      /*WordCountOperator<HashMap<String,ArrayList<KeyValPair>>> counterKeyVal = dag.addOperator("counterKeyVal", new WordCountOperator<HashMap<String,ArrayList<KeyValPair>>>());
      dag.getMeta(counterKeyVal).getMeta(counterKeyVal.input).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
      dag.getMeta(seedEvent).getMeta(seedEvent.keyvalpair_list).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
      dag.addStream("SeedEventGeneratorKeyVal", seedEvent.keyvalpair_list, counterKeyVal.input);

      WordCountOperator<HashMap<String,String>> counterVal = dag.addOperator("counterVal", new WordCountOperator<HashMap<String,String>>());
      dag.getMeta(counterVal).getMeta(counterVal.input).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
      dag.getMeta(seedEvent).getMeta(seedEvent.val_data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
      dag.addStream("SeedEventGeneratorVal", seedEvent.val_data, counterVal.input);

      WordCountOperator<HashMap<String,ArrayList<Integer>>> counterList = dag.addOperator("counterList", new WordCountOperator<HashMap<String,ArrayList<Integer>>>());
      dag.getMeta(counterList).getMeta(counterList.input).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
      dag.getMeta(seedEvent).getMeta(seedEvent.val_list).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
      dag.addStream("SeedEventGeneratorValList", seedEvent.val_list, counterList.input);*/

  }


}
