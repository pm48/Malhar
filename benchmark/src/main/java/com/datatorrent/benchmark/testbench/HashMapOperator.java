/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author prerna
 */
public class HashMapOperator implements InputOperator
{
  public final transient DefaultOutputPort<HashMap<String, Double>> hmap_data = new DefaultOutputPort<HashMap<String, Double>>();
  public final transient DefaultOutputPort<HashMap<String, ArrayList<Integer>>> hmapList_data = new DefaultOutputPort<HashMap<String, ArrayList<Integer>>>();
  public final transient DefaultOutputPort<HashMap<String, HashMap<String,Integer>>> hmapMap_data = new DefaultOutputPort<HashMap<String, HashMap<String,Integer>>>();
  //public final transient DefaultOutputPort<HashMap<String, Integer>> hmapInt_data = new DefaultOutputPort<HashMap<String, Integer>>();

  @Override
  public void emitTuples()
  {
    HashMap<String, Double> hmap = new HashMap<String, Double>();
    HashMap<String, ArrayList<Integer>> hmapList = new HashMap<String, ArrayList<Integer>>();
    HashMap<String, HashMap<String,Integer>> hmapMap = new HashMap<String, HashMap<String,Integer>>();
    ArrayList<Integer> list = new ArrayList<Integer>();
    HashMap<String,Integer> hmapMapTemp = new HashMap<String, Integer>();
    for (int i = 0; i < 1000; i++) {
      hmap.put("key" + i, i + 23.30);
      hmapMapTemp.put("Key" + i, i);
      list.add(i);
    }

    hmapList.put("Key1", list);
    hmapList.put("Key2", list);
    hmapList.put("Key3", list);
    hmapMap.put("Key1", hmapMapTemp);
    hmapMap.put("Key2", hmapMapTemp);
    hmapMap.put("Key3", hmapMapTemp);
    hmap_data.emit(hmap);
    hmapList_data.emit(hmapList);
    hmapMap_data.emit(hmapMap);
    //hmapInt_data.emit(hmapMapTemp);
    list.clear();
    hmapMapTemp.clear();
  }

  @Override
  public void beginWindow(long windowId)
  {
    // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(OperatorContext context)
  {
    // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void teardown()
  {
    // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
