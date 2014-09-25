/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.streamquery;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author prerna
 */
public class SchemaNormalizeOperator implements Operator
{
  protected Map<String, Object> stream1;
  protected Map<String, Object> stream2;

  /**
   * Input port 1.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport1 = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      stream1 = tuple;
      if (stream2 != null) {
        for (String key: stream2.keySet()) {
          if (!(stream1.containsKey(key))) {
            stream1.put(key, null);
          }
        }
      }
      outport.emit(stream1);

    }

  };

  /**
   * Input port 2.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport2 = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      stream2 = tuple;
      if (stream1 != null) {
        for (String key: stream1.keySet()) {
          if (!(stream2.containsKey(key))) {
            stream2.put(key, null);
          }
        }
      }
      outport.emit(stream2);
    }

  };

  /**
   * Output port.
   */
  public final transient DefaultOutputPort<Map<String, Object>> outport
          = new DefaultOutputPort<Map<String, Object>>();

  @Override
  public void beginWindow(long windowId)
  {
    //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void endWindow()
  {
    stream1.clear();
    stream2.clear();
  }

  @Override
  public void setup(OperatorContext context)
  {
    stream1 = new HashMap<String, Object>();
    stream2 = new HashMap<String, Object>();
  }

  @Override
  public void teardown()
  {
    //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
