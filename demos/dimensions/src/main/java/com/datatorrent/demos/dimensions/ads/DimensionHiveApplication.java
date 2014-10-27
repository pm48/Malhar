/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.hds.tfile.TFileImpl;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregator;
import com.datatorrent.lib.statistics.DimensionsComputation;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;

public class DimensionHiveApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, "DimensionHiveApplication");
    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);
    DimensionsComputation<AdInfo, AdInfo.AdInfoAggregateEvent> dimensions = dag.addOperator("DimensionsComputation", new DimensionsComputation<AdInfo, AdInfo.AdInfoAggregateEvent>());
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    String[] dimensionSpecs = new String[] {
        "time=" + TimeUnit.MINUTES,
        "time=" + TimeUnit.MINUTES + ":adUnit",
        "time=" + TimeUnit.MINUTES + ":advertiserId",
        "time=" + TimeUnit.MINUTES + ":publisherId",
        "time=" + TimeUnit.MINUTES + ":advertiserId:adUnit",
        "time=" + TimeUnit.MINUTES + ":publisherId:adUnit",
        "time=" + TimeUnit.MINUTES + ":publisherId:advertiserId",
        "time=" + TimeUnit.MINUTES + ":publisherId:advertiserId:adUnit"
    };

    AdInfoAggregator[] aggregators = new AdInfoAggregator[dimensionSpecs.length];
    for (int i = dimensionSpecs.length; i-- > 0;) {
      AdInfoAggregator aggregator = new AdInfoAggregator();
      aggregator.init(dimensionSpecs[i]);
      aggregators[i] = aggregator;
    }
    dimensions.setAggregators(aggregators);
    HiveOperator hive2 = dag.addOperator("Hive", new HiveOperator());
    hive2.setFilepath("hdfs://localhost:9090/user/hive/a.orc");
    hive2.setAggregator(new AdInfoAggregator());
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath("DimensionHiveApplication");
    hive2.setFileStore(hdsFile);
//    dag.getOperatorMeta("hive2").getAttributes().put(Context.OperatorContext.COUNTERS_AGGREGATOR,
  //      new BasicCounters.LongAggregator< MutableLong >());
     dag.addStream("InputStream", input.outputPort, dimensions.data).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, hive2.input);


  }

}