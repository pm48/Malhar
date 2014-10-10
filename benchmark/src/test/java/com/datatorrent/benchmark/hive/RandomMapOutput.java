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
package com.datatorrent.benchmark.hive;

import java.util.HashMap;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import javax.validation.constraints.Min;

/**
 * Operator that outputs random values in a map.
 *
 * @since 1.0.4
 */
public class RandomMapOutput extends BaseOperator implements InputOperator
{

  public final transient DefaultOutputPort<HashMap<Integer, String>> map_data = new DefaultOutputPort<HashMap<Integer, String>>();

  private int maxCountOfWindows = Integer.MAX_VALUE;
  @Min(1)
  private int tuplesBlast = 1000;
  @Min(1)
  private int tuplesBlastIntervalMillis = 10;
  private int min_value = 0;
  private int max_value = 100;

  public int getMaxvalue()
  {
    return max_value;
  }

  public int getMinvalue()
  {
    return min_value;
  }

  @Min(1)
  public int getTuplesBlast()
  {
    return tuplesBlast;
  }

  @Min(1)
  public int getTuplesBlastIntervalMillis()
  {
    return this.tuplesBlastIntervalMillis;
  }

  public void setMaxvalue(int i)
  {
    max_value = i;
  }

  public void setMinvalue(int i)
  {
    min_value = i;
  }

  public void setTuplesBlast(int i)
  {
    tuplesBlast = i;
  }

  @Override
  public void endWindow()
  {
    if (--maxCountOfWindows == 0) {
      //Thread.currentThread().interrupt();
      throw new RuntimeException(new InterruptedException("Finished generating data."));
    }
  }

  public void setMaxcountofwindows(int i)
  {
    maxCountOfWindows = i;
  }

  public void setTuplesBlastIntervalMillis(int tuplesBlastIntervalMillis)
  {
    this.tuplesBlastIntervalMillis = tuplesBlastIntervalMillis;
  }

  @Override
  public void emitTuples()
  {
    HashMap map = new HashMap<Integer,String>();
    int i=0;
    while (i < tuplesBlast) {
      map.put(i, "Value" + i);
    if (map_data.isConnected()) {
      map_data.emit(map);
    }
    i++;
  }
  }
}
