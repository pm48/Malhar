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
package com.datatorrent.benchmark.stream;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.stream.DevNullCounter;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;



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

  /**
   * Tests both string and non string schema
   * @param dag
   * @param conf
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
    rand.setMinvalue(0);
    rand.setMaxvalue(1000);
    rand.setTuplesBlast(200);
    DevNullCounter oper = dag.addOperator("oper",new DevNullCounter());
    oper.setRollingwindowcount(5);
    dag.addStream("dev", rand.integer_data, oper.data).setLocality(locality);

  }
}
