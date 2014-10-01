/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.LocalMode;
import com.datatorrent.lib.testbench.EventClassifier;
import java.util.HashMap;
import org.junit.Test;

public class EventClassifierAppTest
{
  @Test
  public void testCEventClassifierApp() throws Exception {
  HashMap<String, Double> keymap = new HashMap<String, Double>();
    for(int i=0;i<1000;i++){
      keymap.put("a" + i, 1.0);
      keymap.put("b" + i, 4.0);
      keymap.put("c" + i, 5.0);
    }

    EventClassifier eventInput = new EventClassifier();
    eventInput.setKeyMap(keymap);
    LocalMode.runApp(new EventClassifierApp(), 30000);
   }
}
