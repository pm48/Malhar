/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.stream;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.benchmark.WordCountOperator;
import com.datatorrent.lib.stream.StreamMerger;
import org.apache.hadoop.conf.Configuration;

public class StreamMergeApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    IntegerOperator intInput = dag.addOperator("intInput", new IntegerOperator());
    StreamMerger stream = dag.addOperator("oper", new StreamMerger());
    dag.getMeta(stream).getMeta(stream.data1).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.getMeta(stream).getMeta(stream.data2).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("streammerge1", intInput.integer_data, stream.data1,stream.data2).setLocality(locality);

    WordCountOperator<Integer> counter = dag.addOperator("counter", new WordCountOperator<Integer>());
    dag.getMeta(counter).getMeta(counter.input).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.getMeta(stream).getMeta(stream.out).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("streammerge2", stream.out, counter.input).setLocality(locality);

  }

}
