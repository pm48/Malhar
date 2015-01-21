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

    LOG.info("conf properties are {}" , conf.get("dt.application.HiveInsertBenchmarkingApp.operator.HiveOperator.store.connectionProperties"));
    LOG.info("conf dburl is {}" , conf.get("dt.application.HiveInsertBenchmarkingApp.operator.HiveOperator.store.dbUrl"));
    LOG.info("conf filepath is {}" , conf.get("dt.application.HiveInsertBenchmarkingApp.operator.HiveOperator.store.filepath"));
    LOG.info("permission is {}" , conf.get("dt.application.HiveInsertBenchmarkingApp.operator.RollingFsWriter.filePermission"));
    LOG.info("maximum length is {}" , conf.get("dt.application.HiveInsertBenchmarkingApp.operator.RollingFsWriter.maxLength"));
    LOG.info("tablename is {}" , conf.get("dt.application.HiveInsertBenchmarkingApp.operator.HiveOperator.tablename"));
    HiveInsertBenchmarkingApp app = new HiveInsertBenchmarkingApp();
    LocalMode lm = LocalMode.newInstance();
    try {
      lm.prepareDAG(app, conf);
      LocalMode.Controller lc = lm.getController();
      lc.run(30000);
    }
    catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }

    IOUtils.closeQuietly(inputStream);
  }
}
