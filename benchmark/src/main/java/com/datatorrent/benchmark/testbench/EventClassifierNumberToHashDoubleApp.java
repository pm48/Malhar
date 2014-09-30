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
import com.datatorrent.benchmark.stream.IntegerOperator;
import com.datatorrent.lib.testbench.EventClassifierNumberToHashDouble;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author prerna
 */
public class EventClassifierNumberToHashDoubleApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    WordCountOperator<HashMap<String,Double>> counterString = dag.addOperator("counterString", new WordCountOperator<HashMap<String,Double>>());
    dag.getMeta(counterString).getMeta(counterString.input).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    EventClassifierNumberToHashDouble eventClassify = dag.addOperator("eventClassify", new EventClassifierNumberToHashDouble());
    dag.getMeta(eventClassify).getMeta(eventClassify.data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("eventclassifier1",eventClassify.data,counterString.input).setLocality(locality);
    IntegerOperator intInput = dag.addOperator("intInput", new IntegerOperator());
    dag.addStream("eventclassifier2",intInput.integer_data,eventClassify.event).setLocality(locality);

  }

}
