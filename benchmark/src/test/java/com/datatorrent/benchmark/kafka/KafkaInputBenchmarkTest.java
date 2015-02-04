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
package com.datatorrent.benchmark.kafka;

import com.datatorrent.api.LocalMode;
import com.datatorrent.common.util.DTThrowable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class KafkaInputBenchmarkTest
{
  @Test
  public void testBenchmark()
  {
    Configuration conf = new Configuration();
    InputStream is = null;
    try {
      is = new FileInputStream("src/site/conf/dt-site-kafka.xml");
    }
    catch (FileNotFoundException ex) {
      DTThrowable.rethrow(ex);
    }
    conf.addResource(is);

    LocalMode lma = LocalMode.newInstance();

    try {
      lma.prepareDAG(new KafkaInputBenchmark(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(30000);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
