/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.testbench.EventGenerator;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author prerna
 */
public class EventGeneratorApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    EventGenerator eventGen = dag.addOperator("eventGen", new EventGenerator());
    DevNull<String> devString = dag.addOperator("devString", new DevNull());
    DevNull<HashMap<String,Double>> devMap = dag.addOperator("devMap", new DevNull());
    DevNull<HashMap<String,Number>> devInt= dag.addOperator("devInt", new DevNull());
    dag.addStream("EventGenString", eventGen.string_data,devString.data).setLocality(locality);
    dag.addStream("EventGenMap", eventGen.hash_data,devMap.data).setLocality(locality);
    dag.addStream("EventGenInt",eventGen.count,devInt.data).setLocality(locality);


  }

}
