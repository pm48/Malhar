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
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

public class FilterClassifierApp implements StreamingApplication
{
  //do same as test for this class
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FilterClassifier filter = dag.addOperator("filter", new FilterClassifier());

    HashMap<String, Double> kmap = new HashMap<String, Double>(3);
    kmap.put("a", 1.0);
    kmap.put("b", 4.0);
    kmap.put("c", 5.0);

    ArrayList<Integer> list = new ArrayList<Integer>(3);
    HashMap<String, ArrayList<Integer>> wmap = new HashMap<String, ArrayList<Integer>>(4);
    list.add(60);
    list.add(10);
    list.add(35);
    wmap.put("ia", list);

    list = new ArrayList<Integer>(3);
    list.add(10);
    list.add(75);
    list.add(15);
    wmap.put("ib", list);

    list = new ArrayList<Integer>(3);
    list.add(20);
    list.add(10);
    list.add(70);
    wmap.put("ic", list);

    list = new ArrayList<Integer>(3);
    list.add(50);
    list.add(15);
    list.add(35);
    wmap.put("id", list);

    filter.setKeyMap(kmap);
    filter.setKeyWeights(wmap);
    filter.setPassFilter(10);
    filter.setTotalFilter(100);
    
    HashMapOperator hmapOper = dag.addOperator("hmap", new HashMapOperator());
    DevNull<HashMap<String,Double>> dev = dag.addOperator("dev",  new DevNull());
    dag.addStream("filter1",hmapOper.hmap_data,filter.data).setLocality(locality);
    dag.addStream("filer2",filter.filter,dev.data).setLocality(locality);

  }
}
