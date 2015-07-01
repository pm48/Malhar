/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.converter;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.TestUtils;

public class MapToKeyValuePairConverterTest {

  @Test
  public void MapToKeyValuePairConversion() 
  {
    MapToKeyValuePairConverter<String, Integer> testop = new MapToKeyValuePairConverter<String, Integer>();
    Integer[] values = {1, 2, 3};
    String[] keys = {"a", "b", "c"};
    
    HashMap<String, Integer> inputMap = new HashMap<String, Integer>();
    
    for(int i =0 ; i < 3; i++)
    {
      inputMap.put(keys[i], values[i]);      
    }
    
    CollectorTestSink<KeyValPair<String, Integer>> testsink = new CollectorTestSink<KeyValPair<String, Integer>>();    
    TestUtils.setSink(testop.output, testsink);
    
    testop.beginWindow(0);
    
    testop.input.put(inputMap);
    
    testop.endWindow();

    Assert.assertEquals(3,testsink.collectedTuples.size());
  }
}
