/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.streamquery;

import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.HashMap;
import org.junit.Test;

/**
 *
 * @author prerna
 */
public class SchemaNormalizeOperatorTest
{
  @SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
  public void testSqlSelect()
  {
  	// create operator
		SchemaNormalizeOperator oper = new SchemaNormalizeOperator();
  	CollectorTestSink sink = new CollectorTestSink();
  	oper.outport.setSink(sink);

  	oper.setup(null);
  	oper.beginWindow(1);

  	HashMap<String, Object> tuple = new HashMap<String, Object>();
  	tuple.put("a", 0);
  	tuple.put("b", 1);
  	tuple.put("c", 2);
  	oper.inport1.process(tuple);

  	tuple = new HashMap<String, Object>();
  	tuple.put("a", 1);
  	tuple.put("b", 3);
  	tuple.put("c", 4);
  	oper.inport1.process(tuple);

  	tuple = new HashMap<String, Object>();
  	tuple.put("a", 2);
  	tuple.put("b", 11);
  	oper.inport2.process(tuple);

  	tuple = new HashMap<String, Object>();
  	tuple.put("a", 0);
  	tuple.put("b", 7);
  	tuple.put("c", 8);
  	oper.inport2.process(tuple);

  	tuple = new HashMap<String, Object>();
  	tuple.put("a", 1);
  	tuple.put("b", 5);
  	tuple.put("d", 6);
  	oper.inport1.process(tuple);

    tuple = new HashMap<String, Object>();
  	tuple.put("a", 1);
  	tuple.put("b", 5);
  	oper.inport2.process(tuple);

  	oper.endWindow();
  	oper.teardown();

  	System.out.println(sink.collectedTuples.toString());
  }

}
