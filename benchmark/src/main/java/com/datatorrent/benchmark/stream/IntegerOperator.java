/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.stream;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 *
 * @author prerna
 */
public class IntegerOperator implements InputOperator
{
  public final transient DefaultOutputPort<Integer> integer_data = new DefaultOutputPort<Integer>();

  @Override
  public void emitTuples()
  {
    Integer i = 21;
    for(int j=0;j<1000;j++){
    integer_data.emit(i);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void endWindow()
  {
    //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setup(OperatorContext context)
  {
    //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void teardown()
  {
    //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
