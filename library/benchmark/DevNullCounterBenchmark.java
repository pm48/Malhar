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
package com.datatorrent.lib.stream;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.testbench.DevNullCounter}. <p>
 * <br>
 * oper.process is called a billion times<br>
 * With extremely high throughput it does not impact the performance of any other oper
 * <br>
 * Benchmarks:<br>
 * Object payload benchmarked at over 125 Million/sec
 * <br>
 * DRC checks are validated<br>
 *
 */
public class DevNullCounterBenchmark implements StreamingApplication
{
  private final Locality locality = null;

  private static Logger log = LoggerFactory.getLogger(DevNullCounter.class);

  /**
   * Tests both string and non string schema
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    DevNullCounter oper = new DevNullCounter();
    oper.setRollingwindowcount(5);
    oper.setup(null);

    oper.beginWindow(0);
    long numtuples = 100000000;
    Object o = new Object();
    for (long i = 0; i < numtuples; i++) {
      oper.data.process(o);
    }
    oper.endWindow();
    log.info(String.format("\n*******************************************************\nnumtuples(%d)", numtuples));
  }
}
