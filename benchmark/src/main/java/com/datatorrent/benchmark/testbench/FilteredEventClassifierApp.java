package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.testbench.FilterClassifier;
import com.datatorrent.lib.testbench.FilteredEventClassifier;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
public class FilteredEventClassifierApp implements StreamingApplication
{
  //do same as test for this class
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FilteredEventClassifier filterEvent = dag.addOperator("filterEvent", new FilteredEventClassifier());
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

    filterEvent.setKeyMap(kmap);
    filterEvent.setKeyWeights(wmap);
    filterEvent.setPassFilter(10);
    filterEvent.setTotalFilter(100);
    HashMapOperator hmapOper = dag.addOperator("hmapOper", new HashMapOperator());
    DevNull<HashMap<String, Double>> dev = dag.addOperator("dev", new DevNull());
    dag.addStream("filter1", hmapOper.hmap_data, filterEvent.data).setLocality(locality);
    dag.addStream("filer2", filterEvent.filter, dev.data).setLocality(locality);
  }

}
