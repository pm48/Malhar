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

  protected Set<String> keySet = new HashSet<String>();

  /**
   * Output port.
   */
  public final transient DefaultOutputPort<Map<String, Object>> outport
          = new DefaultOutputPort<Map<String, Object>>();

  public void setKeySet(Set<String> keySet)
  {
    this.keySet = keySet;
  }

  public Set<String> getKeySet()
  {
    return keySet;
  }

  /**
   * Input port 1.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport1 = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      for (String key: keySet) {
        if (!(tuple.containsKey(key))) {
          tuple.put(key, null);
        }
      }
      outport.emit(tuple);

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

      for (String key: keySet) {
        if (!(tuple.containsKey(key))) {
          tuple.put(key, null);
        }
      }
      outport.emit(tuple);

    }

  };

}
