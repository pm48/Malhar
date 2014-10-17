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
package com.datatorrent.benchmark.hive;

import org.junit.Test;
import com.datatorrent.api.LocalMode;
import com.datatorrent.benchmark.HiveFileBenchmarkingApp;
import com.datatorrent.contrib.hive.AbstractHiveOutputOperatorTest;
import com.datatorrent.contrib.hive.HiveMetaStore;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Test the DAG declaration in local mode.
 */
public class HiveFileBenchmarkTest
{
   Logger logger = LoggerFactory.getLogger("HiveFileBenchmarkTest.class");

    Configuration conf = new Configuration();
    InputStream inputStream = getClass().getResourceAsStream("/dt-site-hive.xml");

    //conf.addResource(inputStream);

    HiveMetaStore store = new HiveMetaStore();

   // store.setDbUrl("jdbc:hive://localhost:10000");

   // hiveStore.setConnectionProperties(conf.get("dt.application.HiveOutputBenchmark.operator.hiveOutputOperator.store.connectionProperties"));

   // AbstractHiveOutputOperatorTest.hiveInitializeDatabase(hiveStore);


  LocalMode lm = LocalMode.newInstance();

  @Test
  public void test() throws Exception
  {
    LocalMode.runApp(new HiveFileBenchmarkingApp(), 20000);
  }

}
