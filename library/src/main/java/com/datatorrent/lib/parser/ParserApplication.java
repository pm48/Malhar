/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.parser;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.kafka.AbstractKafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;
import java.nio.ByteBuffer;
import kafka.message.Message;

public class ParserApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);

    KafkaSinglePortStringInputOperator kafkaStringInput = dag.addOperator("KafkaStringInput", KafkaSinglePortStringInputOperator.class);
    Parser parser = dag.addOperator("Parser", Parser.class);

    dag.addStream("Kafka2Parser", kafkaStringInput.outputPort, parser.input);
  }

  public class KafkaSinglePortStringInputOperator extends AbstractKafkaSinglePortInputOperator<byte[]>
  {

    /**
     * Implement abstract method of AbstractActiveMQSinglePortInputOperator
     */
    @Override
    public byte[] getTuple(Message message)
    {
      byte[] bytes = null;
      try {
        ByteBuffer buffer = message.payload();
        bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
      }
      catch (Exception ex) {
        return bytes;
      }
      return bytes;
    }

  }

}
