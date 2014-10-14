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
package com.datatorrent.benchmark;

import com.datatorrent.lib.testbench.RandomWordGenerator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.counters.BasicCounters;
import org.apache.commons.lang.mutable.MutableLong;


import org.apache.hadoop.conf.Configuration;

/**
 * Application used to benchmark HIVE FILE OUTPUT operator
 * The DAG consists of random word generator operator that is
 * connected to HDFS output operator that writes to a file on HDFS.<p>
 *
 * @since 0.9.4
 */

@ApplicationAnnotation(name="HiveFileBenchmarkingApp")
public class HiveFileBenchmarkOperator implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //String filePath = "file:///localhost:8080/user/"
          //  + System.currentTimeMillis();

    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);

    RandomWordGenerator wordGenerator = dag.addOperator("wordGenerator", RandomWordGenerator.class);

    dag.getOperatorMeta("wordGenerator").getMeta(wordGenerator.output).getAttributes().put(PortContext.QUEUE_CAPACITY, 10000);
    dag.getOperatorMeta("wordGenerator").getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 1);

    HiveHDFSOutput hiveHDFSOperator = dag.addOperator("hiveHDFSOperator", new HiveHDFSOutput());
    hiveHDFSOperator.setFilePath("hdfs://localhost:9000/user/c.txt");
    hiveHDFSOperator.setAppend(false);
   // dag.getOperatorMeta("hiveOutputOperator").getAttributes().put(OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    dag.addStream("Generator2HDFSOutput", wordGenerator.outputString, hiveHDFSOperator.input);
  }
}


