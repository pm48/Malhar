/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.LocalMode;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.Test;

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

    conf.get("dt.application.EventGeneratorApp.operator.eventGenerator.keysHelper");
    conf.get("dt.application.EventGeneratorApp.operator.eventGenerator.weightsHelper");
    conf.get("dt.application.EventGeneratorApp.operator.eventGenerator.valuesHelper");
    try {
      lm.prepareDAG(new EventGeneratorApp(), conf);
      LocalMode.Controller lc = lm.getController();
      lc.run(20000);
    }
    catch (Exception ex) {
       logger.info(ex.getCause());
    }
    is.close();
  }
}
