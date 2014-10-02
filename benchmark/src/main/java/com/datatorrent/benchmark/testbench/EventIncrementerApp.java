/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.testbench.EventIncrementer;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;


public class EventIncrementerApp implements StreamingApplication
{
  //implement test
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    EventIncrementer eventInc = dag.addOperator("eventInc", new EventIncrementer());
    ArrayList<String> keys = new ArrayList<String>(2);
    ArrayList<Double> low = new ArrayList<Double>(2);
    ArrayList<Double> high = new ArrayList<Double>(2);
    keys.add("x");
    keys.add("y");
    low.add(1.0);
    low.add(1.0);
    high.add(100.0);
    high.add(100.0);
    eventInc.setKeylimits(keys, low, high);
    eventInc.setDelta(1);
    HashMapOperator hmapOper = dag.addOperator("hmap", new HashMapOperator());
    dag.addStream("eventIncInput1",hmapOper.hmapList_data,eventInc.seed);
    dag.addStream("eventIncInput2",hmapOper.hmapMap_data,eventInc.increment);
    DevNull<HashMap<String,Integer>> dev1= dag.addOperator("dev1", new DevNull());
    DevNull<HashMap<String,String>> dev2= dag.addOperator("dev2", new DevNull());
    dag.addStream("eventIncOutput1",eventInc.count,dev1.data).setLocality(locality);
    dag.addStream("eventIncOutput2",eventInc.data,dev2.data).setLocality(locality);

  }


}
