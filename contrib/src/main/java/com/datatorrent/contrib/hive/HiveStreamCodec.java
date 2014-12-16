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

import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class HiveStreamCodec<T> implements StreamCodec<T>, Externalizable
{
  private static final long serialVersionUID = 201412121604L;

  protected KryoSerializableStreamCodec<T> codec = new KryoSerializableStreamCodec<T>();
  protected HiveInsertOperator<T> hiveOperator = new HiveInsertOperator<T>();

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
  public Object fromByteArray(Slice fragment)
  {
    return codec.fromByteArray(fragment);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Slice toByteArray(T object)
  {
    return codec.toByteArray(object);
  }

  @Override
  public int getPartition(T o)
  {
    return hiveOperator.getHivePartition(o).hashCode();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException
  {
    out.writeObject(hiveOperator);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
  {
    hiveOperator = (HiveInsertOperator<T>)in.readObject();
  }

}
