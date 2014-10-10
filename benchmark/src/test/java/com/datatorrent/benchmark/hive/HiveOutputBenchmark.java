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

package com.datatorrent.benchmark.hive;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationAnnotation(name="HiveOutputBenchmark")
public class HiveOutputBenchmark implements StreamingApplication
{
  private static transient final Logger LOG = LoggerFactory.getLogger(HiveOutputBenchmark.class);

  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final int MAX_WINDOW_COUNT = 10000;
  public static final int TUPLE_BLAST_MILLIS = 75;
  public static final int TUPLE_BLAST = 1000;


  private static final Locality LOCALITY = null;



  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomMapOutput randomMapGenerator = dag.addOperator("randomMapGenerator", new RandomMapOutput());
    randomMapGenerator.setMaxcountofwindows(MAX_WINDOW_COUNT);
    randomMapGenerator.setTuplesBlastIntervalMillis(TUPLE_BLAST_MILLIS);
    randomMapGenerator.setTuplesBlast(TUPLE_BLAST);

    LOG.debug("Before making output operator");
    HiveOutputOperator hiveOutputOperator = dag.addOperator("hiveOutputOperator",
                                                                new HiveOutputOperator());
    LOG.debug("After making output operator");

    hiveOutputOperator.setBatchSize(DEFAULT_BATCH_SIZE);

    dag.addStream("hiveConnector",randomMapGenerator.map_data,
                  hiveOutputOperator.input);
  }
}
