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
package com.datatorrent.lib.streamquery;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.testbench.CountTestSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaNormalizeOperatorTest
{
  @SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
  public void testSqlSelect()
  {
    Logger logger = LoggerFactory.getLogger("SchemaNormalizeOperatorTest.class");
  	// create operator
		SchemaNormalizeOperator oper = new SchemaNormalizeOperator();
  	CollectorTestSink sink = new CollectorTestSink();
  	oper.outport.setSink(sink);

  	oper.setup(null);
  	oper.beginWindow(1);

  	HashMap<String, Object> tuple1 = new HashMap<String, Object>();
    HashMap<String, Object> tuple2 = new HashMap<String, Object>();

    tuple1.put("a", 0);
  	tuple1.put("b", 1);
  	tuple1.put("c", 2);
    tuple2.put("a", 2);
  	tuple2.put("b", 11);
    tuple2.put("d", 6);
    tuple2.put("c", 8);

    for(int i=0;i<100;i++){
  	oper.inport1.process(tuple1);

  	oper.inport2.process(tuple2);
    }
  	/*tuple = new HashMap<String, Object>();
  	tuple.put("a", 0);
  	tuple.put("b", 7);
  	tuple.put("c", 8);
  	oper.inport2.process(tuple);

  	tuple = new HashMap<String, Object>();
  	tuple.put("a", 1);
  	tuple.put("b", 5);
  	tuple.put("d", 6);
    tuple.put("d", 6);
  	oper.inport1.process(tuple);

    tuple = new HashMap<String, Object>();
  	tuple.put("a", 1);
  	tuple.put("b", 5);
  	oper.inport2.process(tuple);*/

  	oper.endWindow();
  	oper.teardown();

    logger.info("numer of emitted tuples" + sink.collectedTuples);
  	Assert.assertEquals("number emitted tuples",201,sink.collectedTuples.size());
  }

}
