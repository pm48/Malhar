/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.testbench.FilterClassifier;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Chetan Narsude  <change_this_by_going_to_Tools-Options-Settings@datatorrent.com>
 */
public class FilterClassifierApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FilterClassifier filter = dag.addOperator("filter", new FilterClassifier());
    HashMapOperator hmapOper = dag.addOperator("hmap", new HashMapOperator());
    DevNull<HashMap<String,Double>> dev = dag.addOperator("dev",  new DevNull());
    dag.addStream("filter1",hmapOper.hmap_data,filter.data).setLocality(locality);
    dag.addStream("filer2",filter.filter,dev.data).setLocality(locality);

  }
}
