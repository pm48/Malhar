/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.kafka;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaJsonEncoder
 *
 * @since 2.0.0
 */
public class KafkaJsonEncoder implements kafka.serializer.Encoder<Object>
{
  private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonEncoder.class);
  private final ObjectMapper mapper = new ObjectMapper();

  public KafkaJsonEncoder(kafka.utils.VerifiableProperties props)
  {
  }

  @Override
  public byte[] toBytes(Object arg0)
  {
    try {
      return mapper.writeValueAsBytes(arg0);
    } catch (Exception e) {
      LOG.error("Failed to encode {}", arg0, e);
      throw new RuntimeException(e);
    }
  }
}

