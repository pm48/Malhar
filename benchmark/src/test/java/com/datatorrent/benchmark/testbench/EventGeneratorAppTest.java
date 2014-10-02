/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.LocalMode;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.Test;

/**
 *
 * @author Chetan Narsude  <change_this_by_going_to_Tools-Options-Settings@datatorrent.com>
 */
public class EventGeneratorAppTest
{
  @Test
  public void testEventGeneratorApp() throws Exception
  {
    Logger logger = Logger.getLogger(EventGeneratorAppTest.class);
    Configuration conf = new Configuration();

    LocalMode lm = LocalMode.newInstance();

    InputStream is = getClass().getResourceAsStream("/dt-site-testbench.xml");
    conf.addResource(is);
    FileSystem fs = FileSystem.get(conf);
    conf.get("dt.application.EventGeneratorApp.operator.eventGenerator.keysHelper");
    conf.get("dt.application.EventGeneratorApp.operator.eventGenerator.weightsArray");
    conf.get("dt.application.EventGeneratorApp.operator.eventGenerator.valuesArray");
    try {
      lm.prepareDAG(new EventGeneratorApp(), conf);
      LocalMode.Controller lc = lm.getController();
      //lc.setHeartbeatMonitoringEnabled(false);
      lc.run(20000);
    }
    catch (Exception ex) {
       logger.info(ex.getCause());
    }
    is.close();
  }
}
