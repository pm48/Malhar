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
package com.datatorrent.benchmark.fs;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.fs.HdfsWordInputOperator;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.benchmark.WordCountOperator;

import org.apache.hadoop.conf.Configuration;

/**
 * Application used to benchmark FS input operator<p>
 *
 * The DAG consists of FS input operator that is connected to word count operator.
 *
 * @since 0.9.4
 */

@ApplicationAnnotation(name="FSInputOperatorBenchmarkingApp")
public abstract class FSInputOperatorBenchmark
{
  static abstract class AbstractApplication implements StreamingApplication
  {
   // public static final int QUEUE_CAPACITY = 32 * 1024;

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      HdfsWordInputOperator wordGenerator =  dag.addOperator("wordGenerator", HdfsWordInputOperator.class);
      wordGenerator.setFilePath("hdfs:///user/hadoop/hdfsOperatorBenchmarking/2/transactions.out.part0");

      WordCountOperator<byte[]> counter = dag.addOperator("counter", new WordCountOperator<byte[]>());

      dag.addStream("FSInputOperator2Counter", wordGenerator.output, counter.input).setLocality(getLocality());
    }

    public abstract Locality getLocality();

  }

  /**
   * Let the engine decide how to best place the 2 operators.
   */
  @ApplicationAnnotation(name="FSInputOperatorBenchmarkNoLocality")
  public static class NoLocality extends AbstractApplication
  {
    @Override
    public Locality getLocality()
    {
      return null;
    }
  }

  /**
   * Place the 2 operators so that they are in the same Rack.
   */
  @ApplicationAnnotation(name="FSInputOperatorBenchmarkRackLocality")
  public static class RackLocal extends AbstractApplication
  {
    @Override
    public Locality getLocality()
    {
      return Locality.RACK_LOCAL;
    }
  }

  /**
   * Place the 2 operators so that they are in the same node.
   */
  @ApplicationAnnotation(name="FSInputOperatorBenchmarkNodeLocality")
  public static class NodeLocal extends AbstractApplication
  {
    @Override
    public Locality getLocality()
    {
      return Locality.NODE_LOCAL;
    }
  }

  /**
   * Place the 2 operators so that they are in the same container.
   */
  @ApplicationAnnotation(name="FSInputOperatorBenchmarkContainerLocality")
  public static class ContainerLocal extends AbstractApplication
  {
    @Override
    public Locality getLocality()
    {
      return Locality.CONTAINER_LOCAL;
    }
  }

  /**
   * Place the 2 operators so that they are in the same thread.
   */
  @ApplicationAnnotation(name="FSInputOperatorBenchmarkThreadLocality")
  public static class ThreadLocal extends AbstractApplication
  {
    @Override
    public Locality getLocality()
    {
      return Locality.THREAD_LOCAL;
    }
  }

}


