/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.stream;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.stream.StreamDuplicater;
import org.apache.hadoop.conf.Configuration;

public class StreamDuplicaterApp implements StreamingApplication
{
   private final Locality locality = null;
   public static final int QUEUE_CAPACITY = 16 * 1024;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
   // RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
   // rand.setMinvalue(0);
   // rand.setMaxvalue(999999);
   // rand.setTuplesBlastIntervalMillis(50);
   // dag.getMeta(rand).getMeta(rand.integer_data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    IntegerOperator intInput = dag.addOperator("intInput", new IntegerOperator());
    StreamDuplicater stream = dag.addOperator("stream", new StreamDuplicater());
    dag.getMeta(stream).getMeta(stream.data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("streamdup1", intInput.integer_data, stream.data).setLocality(locality);
    DevNull<Integer> dev1 = dag.addOperator("dev1", new DevNull());
    DevNull<Integer> dev2 = dag.addOperator("dev2", new DevNull());
    dag.addStream("streamdup2",stream.out1,dev1.data).setLocality(locality);
    dag.addStream("streamdup3",stream.out2,dev2.data).setLocality(locality);

  }

}
