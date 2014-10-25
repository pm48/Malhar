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

import com.datatorrent.api.LocalMode;
import com.datatorrent.benchmark.HiveMapBenchmarkApp;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.contrib.hive.AbstractHiveOutputOperatorTest;
import com.datatorrent.contrib.hive.HiveMetaStore;
import java.io.InputStream;
import java.sql.SQLException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class HiveMapBenchmarkTest
{
  @Test
  public void testMethod() throws SQLException
  {
    Configuration conf = new Configuration();
    InputStream inputStream = getClass().getResourceAsStream("/dt-site-hive.xml");
    conf.addResource(inputStream);

    HiveMetaStore store = new HiveMetaStore();
    store.setDbUrl(conf.get("rootDbUrl"));
    store.setConnectionProperties(conf.get("dt.application.HiveMapBenchmarkApp.operator.HiveHDFSMapOutput.store.connectionProperties"));

    HiveMapBenchmarkApp app = new HiveMapBenchmarkApp();
    AbstractHiveOutputOperatorTest.hiveInitializeDatabase(store);
    LocalMode lm = LocalMode.newInstance();
    try {
      lm.prepareDAG(app, conf);
      LocalMode.Controller lc = lm.getController();
      lc.run(50000);
    }
    catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }

    IOUtils.closeQuietly(inputStream);
  }



}