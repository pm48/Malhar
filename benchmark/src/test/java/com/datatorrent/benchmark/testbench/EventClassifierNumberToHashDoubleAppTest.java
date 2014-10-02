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

/**
 *
 * @author Chetan Narsude  <change_this_by_going_to_Tools-Options-Settings@datatorrent.com>
 */
public class EventClassifierNumberToHashDoubleAppTest
{
  @Test
  public void testEventClassifierNumberToHashDoubleApp() throws Exception
  {
    Logger logger = Logger.getLogger(EventClassifierNumberToHashDoubleAppTest.class);
    Configuration conf = new Configuration();
    LocalMode lm = LocalMode.newInstance();
    InputStream is = getClass().getResourceAsStream("/dt-site-testbench.xml");
    conf.addResource(is);
    conf.get("dt.application.EventClassifierNumberToHashDoubleApp.operator.eventClassify.key_keys");
    conf.get("dt.application.EventClassifierNumberToHashDoubleApp.operator.eventClassify.s_start");
    conf.get("dt.application.EventClassifierNumberToHashDoubleApp.operator.eventClassify.s_end");
    try {
      lm.prepareDAG(new EventClassifierNumberToHashDoubleApp(), conf);
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
