/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.LocalMode;
import com.datatorrent.lib.testbench.EventClassifier;
import java.util.ArrayList;
import java.util.HashMap;
import org.junit.Test;

public class EventClassifierAppTest
{
  @Test
  public void testEventClassifierApp() throws Exception
  {
    HashMap<String, Double> keymap = new HashMap<String, Double>();
    keymap.put("a", 1.0);
    keymap.put("b", 4.0);
    keymap.put("c", 5.0);

    EventClassifier eventInput = new EventClassifier();
    eventInput.setKeyMap(keymap);
    eventInput.setOperationReplace();

    HashMap<String, ArrayList<Integer>> wmap = new HashMap<String, ArrayList<Integer>>();
    ArrayList<Integer> list = new ArrayList<Integer>(3);
    list.add(60);
    list.add(10);
    list.add(35);
    wmap.put("ia", list);
    list = new ArrayList<Integer>(3);
    list.add(10);
    list.add(75);
    list.add(15);
    wmap.put("ib", list);
    list = new ArrayList<Integer>(3);
    list.add(20);
    list.add(10);
    list.add(70);
    wmap.put("ic", list);
    list = new ArrayList<Integer>(3);
    list.add(50);
    list.add(15);
    list.add(35);
    wmap.put("id", list);
    eventInput.setKeyWeights(wmap);

    LocalMode.runApp(new EventClassifierApp(), 30000);
  }

}
