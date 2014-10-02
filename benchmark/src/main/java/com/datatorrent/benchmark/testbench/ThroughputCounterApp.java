/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.testbench.ThroughputCounter;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
//3,608..expected: String schema generates over 11 Million tuples/sec<br>HashMap schema generates over 1.7 Million tuples/sec<
public class ThroughputCounterApp implements StreamingApplication
{
  public static final int QUEUE_CAPACITY = 16 * 1024;
  private final Locality locality = null;
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    ThroughputCounter counter = dag.addOperator("counter", new ThroughputCounter());
    HashMapOperator oper = dag.addOperator("oper", new HashMapOperator());
    DevNull<HashMap<String,Number>> dev = dag.addOperator("dev", new DevNull());
    dag.addStream("count1",oper.hmapInt_data,counter.data).setLocality(locality);
    dag.addStream("count2",counter.count,dev.data).setLocality(locality);

  }

}
