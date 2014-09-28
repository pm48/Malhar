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
import com.datatorrent.lib.testbench.EventGenerator;
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
    WordCountOperator<String> counterString = dag.addOperator("counterString", new WordCountOperator<String>());
    dag.getMeta(counterString).getMeta(counterString.input).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    EventGenerator eventGen = dag.addOperator("eventGen", new EventGenerator());
  }

}
