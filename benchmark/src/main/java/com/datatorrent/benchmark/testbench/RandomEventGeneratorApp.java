/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author prerna
 */
public class RandomEventGeneratorApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomEventGenerator random = dag.addOperator("random", new RandomEventGenerator());
    DevNull<Integer> dev1 = dag.addOperator("dev1", new DevNull());
    DevNull<String> dev2 = dag.addOperator("dev2", new DevNull());
    dag.addStream("random1",random.integer_data,dev1.data).setLocality(locality);
    dag.addStream("random2",random.string_data,dev2.data).setLocality(locality);
  }

}
