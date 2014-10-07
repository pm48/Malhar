/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.streamquery;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SchemaNormalizeOperator extends BaseOperator
{
  protected Map<String, Object> buffer;
  protected Map<String, Object> stream2;
  protected Set<String> keySet1 = new HashSet<String>();
  protected Set<String> keySet2 = new HashSet<String>();
  protected boolean firstTime = true;

  public void setKeySet1(Set<String> keySet)
  {
    this.keySet1 = keySet;
  }

  public Set<String> getKeySet1()
  {
    return keySet1;
  }

 public void setKeySet2(Set<String> keySet)
  {
    this.keySet2 = keySet;
  }

  public Set<String> getKeySet2()
  {
    return keySet2;
  }

  /**
   * Input port 1.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport1 = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      if (stream2 == null && firstTime) {
        buffer.putAll(tuple);
      }
      else if (stream2 != null) {
        for (String key: stream2.keySet()) {
          if (!(buffer.containsKey(key))) {
            buffer.put(key, null);
          }
        }
        firstTime = false;

      }

      if(!firstTime)
        outport.emit(buffer);

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
      outport.emit(stream2);
    }

  };


 @Override
  public void setup(OperatorContext arg0)
  {
    buffer = new HashMap<String, Object>();
    stream2 = new HashMap<String, Object>();
  }
  /**
   * Output port.
   */
  public final transient DefaultOutputPort<Map<String, Object>> outport
          = new DefaultOutputPort<Map<String, Object>>();

}
