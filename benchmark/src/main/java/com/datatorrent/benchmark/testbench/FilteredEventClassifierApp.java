package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.testbench.FilterClassifier;
import com.datatorrent.lib.testbench.FilteredEventClassifier;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
/**
 *
 * @author Chetan Narsude  <change_this_by_going_to_Tools-Options-Settings@datatorrent.com>
 */
public class FilteredEventClassifierApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FilteredEventClassifier filterEvent = dag.addOperator("filterEvent", new FilteredEventClassifier());
    HashMapOperator hmapOper = dag.addOperator("hmap", new HashMapOperator());
    DevNull<HashMap<String,Double>> dev = dag.addOperator("dev",  new DevNull());
    dag.addStream("filter1",hmapOper.hmap_data,filterEvent.data).setLocality(locality);
    dag.addStream("filer2",filterEvent.filter,dev.data).setLocality(locality);
  }

}
