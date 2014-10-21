/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.testbench.RandomWordGenerator;

import org.apache.hadoop.conf.Configuration;

/**
 * Application used to benchmark HIVE FILE OUTPUT operator
 * The DAG consists of random word generator operator that is
 * connected to HDFS output operator that writes to a file on HDFS.<p>
 *
 */
@ApplicationAnnotation(name = "HiveFileBenchmarkingApp")
public class HiveFileBenchmarkingApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);

    RandomWordGenerator WordGenerator = dag.addOperator("WordGenerator", RandomWordGenerator.class);

    dag.getOperatorMeta("WordGenerator").getMeta(WordGenerator.outputString).getAttributes().put(PortContext.QUEUE_CAPACITY, 10000);
    dag.getOperatorMeta("WordGenerator").getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 1);

    HiveHDFSOutput HiveHdfsOperator = dag.addOperator("HiveHdfsOperator", new HiveHDFSOutput());
    HiveHdfsOperator.setFilename("HiveInsertString");
    HiveHdfsOperator.setAppend(false);
    dag.addStream("Generator2HDFSOutput", WordGenerator.outputString, HiveHdfsOperator.input);
  }

}
