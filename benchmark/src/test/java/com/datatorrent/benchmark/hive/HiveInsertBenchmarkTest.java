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
import com.datatorrent.benchmark.HiveInsertBenchmarkingApp;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.contrib.hive.AbstractHiveOutputOperatorTest;
import com.datatorrent.contrib.hive.HiveMetaStore;
import java.io.InputStream;
import java.sql.SQLException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveInsertBenchmarkTest
{
  private static final Logger LOG = LoggerFactory.getLogger(HiveInsertBenchmarkTest.class);

  @Test
  public void testMethod() throws SQLException
  {
    Configuration conf = new Configuration();
    InputStream inputStream = getClass().getResourceAsStream("/dt-site-hive.xml");
    conf.addResource(inputStream);

    HiveMetaStore store = new HiveMetaStore();
    store.setDbUrl(conf.get("rootDbUrl"));
    store.setConnectionProperties(conf.get("dt.application.HiveInsertBenchmarkingApp.operator.HiveInsertOperator.store.connectionProperties"));
    LOG.info("conf properties are" + conf.get("dt.application.HiveInsertBenchmarkingApp.operator.HiveInsertOperator.store.connectionProperties"));
    LOG.info("conf dburl is" + conf.get("dt.application.HiveInsertBenchmarkingApp.operator.HiveInsertOperator.store.dbUrl"));
    LOG.info("conf filepath is" + conf.get("dt.application.HiveInsertBenchmarkingApp.operator.HiveInsertOperator.store.filepath"));
    HiveInsertBenchmarkingApp app = new HiveInsertBenchmarkingApp();
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
