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
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name = "HiveMapBenchmarkApp")
public class HiveMapBenchmarkApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);

    RandomMapOutput MapGenerator = dag.addOperator("MapGenerator", RandomMapOutput.class);

    dag.getOperatorMeta("MapGenerator").getMeta(MapGenerator.map_data).getAttributes().put(PortContext.QUEUE_CAPACITY, 10000);
    dag.getOperatorMeta("MapGenerator").getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 1);

    HiveHDFSMapOutput HiveHdfsMapOperator = dag.addOperator("HiveHdfsMapOperator", new HiveHDFSMapOutput());
    HiveHdfsMapOperator.setFilename("HiveInsertMap");
    HiveHdfsMapOperator.setAppend(false);
    dag.addStream("MapGenerator2HiveOutput", MapGenerator.map_data, HiveHdfsMapOperator.input);
  }

}
