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
package com.datatorrent.lib.io.fs;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.ProcessingMode;
import static com.datatorrent.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.APP_ID;
import static com.datatorrent.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.OPERATOR_ID;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.junit.Test;

public class HDFSRollingOperatorTest
{

  @Test
  public void HDFSRollingOperatorTest()
  {
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);
    HiveInsertOperator<String> outputOperator = new HiveInsertOperator<String>();
    HiveInsertOperator newOp = new HiveInsertOperator();
    outputOperator.setFilepath("/tmp");
   // ArrayOutOfBounds Exception seen if hdfs path is passed here.
    // outputOperator.setFilepath("hdfs://localhost:9000/user/hive");

    outputOperator.setup(context);
    for (int wid = 0, total = 0;
            wid < 10;
            wid++) {
      outputOperator.beginWindow(wid);
      /*if (wid == 5) {
       outputOperator.committed(wid - 2);
       //outputOperator.checkpointed(wid - 3);
       }*/
      for (int tupleCounter = 0;
              tupleCounter < 10 && total < 100;
              tupleCounter++, total++) {
        outputOperator.processTuple(111 + "");
      }

      if (wid == 4) {
        Kryo kryo = new Kryo();
        FieldSerializer<HiveInsertOperator> f1 = (FieldSerializer<HiveInsertOperator>)kryo.getSerializer(HiveInsertOperator.class);
        FieldSerializer<HDFSRollingOutputOperator> f2 = (FieldSerializer<HDFSRollingOutputOperator>)kryo.getSerializer(HDFSRollingOutputOperator.class);
        f1.setCopyTransient(false);
        f2.setCopyTransient(false);

        newOp = kryo.copy(outputOperator);
      }

      if (wid == 6) {
        outputOperator.checkpointed(wid - 2);
        outputOperator.teardown();
        newOp.setup(context);
        newOp.beginWindow(4);
        for (int i = 271; i < 300; i++) {
          newOp.processTuple(111 + "");
        }
        newOp.endWindow();
        newOp.teardown();
        break;
      }
      outputOperator.endWindow();

    }

  }

}
