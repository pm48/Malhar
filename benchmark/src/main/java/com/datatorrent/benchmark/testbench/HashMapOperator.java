/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import java.util.HashMap;

/**
 *
 * @author prerna
 */
public class HashMapOperator implements InputOperator
{
  public final transient DefaultOutputPort<HashMap<String, Double>> string_data = new DefaultOutputPort<HashMap<String, Double>>();

  @Override
  public void emitTuples()
  {
    HashMap<String, Double> hmap = new HashMap<String, Double>();
    for (int i = 0; i < 1000; i++) {
      hmap.put("key1", 23.30);
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
    // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
