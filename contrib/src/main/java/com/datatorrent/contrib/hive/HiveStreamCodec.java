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
package com.datatorrent.contrib.hive;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.*;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * An optional stream codec which implements Externalizable interface.
 * This was done so that user can implement their own serialization/deserialization functions.
 */
public class HiveStreamCodec<T> extends KryoSerializableStreamCodec<T> implements Externalizable
{
  private static final long serialVersionUID = 201412121604L;
  private static final Logger logger = LoggerFactory.getLogger(HiveStreamCodec.class);
  protected HiveInsertOperator<T> hiveOperator;

  /*
   * mandatory public no-arg constructor
   */
  public HiveStreamCodec()
  {
    super();
  }

  public void setHiveOperator(HiveInsertOperator<T> hiveOperator)
  {
    this.hiveOperator = hiveOperator;
  }

  @Override
  public int getPartition(T o)
  {
    logger.info("hivePartition in stream codec is" + hiveOperator.hivePartitions.toString());
    logger.info("hiveOperator used is" + hiveOperator.getName());
    return hiveOperator.getHivePartition(o).hashCode();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException
  {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ObjectOutputStream obj = new ObjectOutputStream(os);
    Output output = new Output(obj);
    kryo.writeClassAndObject(output, hiveOperator);
    byte[] outBytes = output.toBytes();
    out.writeInt(outBytes.length);
    out.write(outBytes,0,outBytes.length);
    out.flush();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
  {
    int size = in.readInt();
    byte[] data = new byte[size];
    in.readFully(data);
    String hex = Hex.encodeHexString(data);
    Input input = new Input(data,0,size);
    input.setBuffer(data);
    hiveOperator = (HiveInsertOperator<T>)kryo.readClassAndObject(input);
  }

}
