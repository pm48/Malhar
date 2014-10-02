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
  public final transient DefaultOutputPort<HashMap<String, HashMap<String, Integer>>> hmapMap_data = new DefaultOutputPort<HashMap<String, HashMap<String, Integer>>>();
  public final transient DefaultOutputPort<HashMap<String, Integer>> hmapInt_data = new DefaultOutputPort<HashMap<String, Integer>>();

  @Override
  public void emitTuples()
  {
    HashMap<String, Double> hmap = new HashMap<String, Double>();
    HashMap<String, ArrayList<Integer>> hmapList = new HashMap<String, ArrayList<Integer>>();
    HashMap<String, HashMap<String, Integer>> hmapMap = new HashMap<String, HashMap<String, Integer>>();
    ArrayList<Integer> list = new ArrayList<Integer>();
    HashMap<String, Integer> hmapMapTemp = new HashMap<String, Integer>();
    int numTuples = 1000;
    Integer aval = 1000;
    Integer bval = 100;

   /* HashMap<String, Object> stuple = new HashMap<String, Object>(1);
    String seed1 = "a";
    ArrayList val = new ArrayList();
    val.add(10);
    val.add(20);
    stuple.put(seed1, val);*/

    for (int i = 0; i < numTuples; i++) {
      hmap.clear();
      list.clear();
      hmapList.clear();
      hmapMapTemp.clear();
      hmap.put("ia", 2.0);
      hmap.put("ib", 20.0);
      hmap.put("ic", 1000.0);
      hmap.put("id", 1000.0);
      hmap_data.emit(hmap);
      list.add(i);
      hmapList.put("Key" +i, list);
      hmapList_data.emit(hmapList);
      hmapMapTemp.put("a", aval);
      hmapMapTemp.put("b", bval);
      hmapInt_data.emit(hmapMapTemp);
      hmapMap.put("a", hmapMapTemp);
      hmapMap_data.emit(hmapMap);
    }

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
