/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import static com.datatorrent.benchmark.ApplicationFixed.QUEUE_CAPACITY;
import com.datatorrent.benchmark.WordCountOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.testbench.SeedEventGenerator;
import org.apache.hadoop.conf.Configuration;


/**
 *
 * @author prerna
 */
public class SeedEventGeneratorApp implements StreamingApplication
{
  private final Locality locality = null;
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
      SeedEventGenerator seedEvent = dag.addOperator("seedEvent", new SeedEventGenerator());
      seedEvent.addKeyData("x", 0, 9);
      seedEvent.addKeyData("y", 0, 9);
      seedEvent.addKeyData("gender", 0, 1);
      seedEvent.addKeyData("age", 10, 19);
      WordCountOperator<byte[]> counter = dag.addOperator("Counter", new WordCountOperator<byte[]>());
      dag.getMeta(counter).getMeta(counter.input).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);

      dag.addStream("SeedEventGenerator2Counter", seedEvent.string_data ,  counter.input).setLocality(locality);
  }


}
