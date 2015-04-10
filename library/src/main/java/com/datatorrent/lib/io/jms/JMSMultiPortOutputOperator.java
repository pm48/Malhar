/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.io.jms;

import com.datatorrent.api.DefaultInputPort;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import javax.jms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMSMultiPortOutputOperator extends AbstractJMSOutputOperator
{
  /**
   * Convert to and send message.
   *
   * @param tuple
   */
  protected void processTuple(Object tuple)
  {
    sendMessage(tuple);
  }

  /**
   * This is an input port which receives map tuples to be written out to an JMS message bus.
   */
  public final transient DefaultInputPort<Map> inputMapPort = new DefaultInputPort<Map>()
  {
    @Override
    public void process(Map tuple)
    {
      processTuple(tuple);
    }

  };

  /**
   * This is an input port which receives byte array tuples to be written out to an JMS message bus.
   */
  public final transient DefaultInputPort<byte[]> inputByteArrayPort = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] tuple)
    {
      processTuple(tuple);
    }

  };

  /**
   * This is an input port which receives Serializable tuples to be written out to an JMS message bus.
   */
  public final transient DefaultInputPort<Serializable> inputObjectPort = new DefaultInputPort<Serializable>()
  {
    @Override
    public void process(Serializable tuple)
    {
      processTuple(tuple);
    }

  };

  /**
   * This is an input port which receives string tuples to be written out to an JMS message bus.
   */
  public final transient DefaultInputPort<String> inputStringTypePort = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      processTuple(tuple);
    }

  };

  @Override
  protected Message createMessage(Object tuple)
  {
    try {
      if (tuple instanceof Message) {
        return (Message)tuple;
      }
      else if (tuple instanceof String) {
        return createMessageForString((String)tuple);
      }
      /*else if(tuple.getClass().isArray()){
         Class<?> componentType;
        componentType = tuple.getClass().getComponentType();
        if(byte.class.isAssignableFrom(componentType)) {
        return createMessageForByteArray((byte[])tuple);
      }
        else
          return (Message)tuple;
      }*/
      else if(tuple instanceof byte[]){
        return createMessageForByteArray((byte[])tuple);
      }
      else if (tuple instanceof Map) {
        return createMessageForMap((Map)tuple);
      }
      else if (tuple instanceof Serializable) {
        logger.debug("serializable integer {}", tuple);
        return createMessageForSerializable(((Serializable)tuple));
      }
      else {
        throw new RuntimeException("Cannot convert object of type "
                + tuple.getClass() + "] to JMS message. Supported message "
                + "payloads are: String, byte array, Map<String,?>, Serializable object.");
      }
    }
    catch (JMSException ex) {
      logger.error(ex.getLocalizedMessage());
      throw new RuntimeException(ex);
    }
  }

  /**
   * Create a JMS TextMessage for the given String.
   *
   * @param text the String to convert
   * @param session current JMS session
   * @return the resulting message
   * @throws JMSException if thrown by JMS methods
   */
  private Message createMessageForString(String text) throws JMSException
  {
    logger.debug("text is {}", text);
    return getSession().createTextMessage(text);
  }

  /**
   * Create a JMS BytesMessage for the given byte array.
   *
   * @param bytes the byte array to convert
   * @param session current JMS session
   * @return the resulting message
   * @throws JMSException if thrown by JMS methods
   */
  private Message createMessageForByteArray(byte[] bytes) throws JMSException
  {
    BytesMessage message = getSession().createBytesMessage();
    message.writeBytes(bytes);
    logger.debug("bytes message is {}", message);
    return message;
  }

  /**
   * Create a JMS MapMessage for the given Map.
   *
   * @param map the Map to convert
   * @param session current JMS session
   * @return the resulting message
   * @throws JMSException if thrown by JMS methods
   */
  private Message createMessageForMap(Map map) throws JMSException
  {
    MapMessage message = getSession().createMapMessage();
    for (Iterator it = map.entrySet().iterator(); it.hasNext();) {
      Map.Entry entry = (Map.Entry)it.next();
      if (!(entry.getKey() instanceof String)) {
        throw new RuntimeException("Cannot convert non-String key of type ["
                + entry.getKey().getClass() + "] to JMS MapMessage entry");
      }
      message.setObject((String)entry.getKey(), entry.getValue());
    }
    return message;
  }

  /**
   * Create a JMS ObjectMessage for the given Serializable object.
   *
   * @param object the Serializable object to convert
   * @param session current JMS session
   * @return the resulting message
   * @throws JMSException if thrown by JMS methods
   */
  private Message createMessageForSerializable(Serializable object) throws JMSException
  {
    return getSession().createObjectMessage(object);
  }

  private static final Logger logger = LoggerFactory.getLogger(JMSMultiPortOutputOperator.class);

}
