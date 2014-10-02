/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.testbench.EventClassifier;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
//1,089,063 tuples, expected :3 Million tuples/sec

@ApplicationAnnotation(name = "EventClassifierApp")
public class EventClassifierApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    HashMap<String, Double> keymap = new HashMap<String, Double>();
    keymap.put("a", 1.0);
    keymap.put("b", 4.0);
    keymap.put("c", 5.0);
    HashMap<String, ArrayList<Integer>> wmap = new HashMap<String, ArrayList<Integer>>();
    ArrayList<Integer> list = new ArrayList<Integer>(3);
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
    HashMapOperator hmapOper = dag.addOperator("hmap", new HashMapOperator());
    dag.getMeta(hmapOper).getMeta(hmapOper.hmap_data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    EventClassifier eventClassifier = dag.addOperator("eventClassifier", new EventClassifier());
    eventClassifier.setKeyMap(keymap);
    eventClassifier.setOperationReplace();
    eventClassifier.setKeyWeights(wmap);
    dag.getMeta(eventClassifier).getMeta(eventClassifier.data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("eventtest1", hmapOper.hmap_data, eventClassifier.event).setLocality(locality);
    DevNull<HashMap<String, Double>> dev = dag.addOperator("dev", new DevNull());
    dag.addStream("eventtest2", eventClassifier.data, dev.data).setLocality(locality);

  }

}
