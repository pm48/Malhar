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
package com.datatorrent.contrib.couchbase;

import java.io.IOException;
import java.util.List;

import com.datatorrent.lib.db.AbstractStoreInputOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.couchbase.client.vbucket.config.VBucket;

/**
 * AbstractCouchBaseInputOperator which extends AbstractStoreInputOperator.
 * Classes extending from this operator should implement the abstract functionality of getTuple and getKeys.
 */
public abstract class AbstractCouchBaseInputOperator<T> extends AbstractStoreInputOperator<T, CouchBaseStore> implements Partitioner<AbstractCouchBaseInputOperator<T>>
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractCouchBaseInputOperator.class);
  protected int partitionMask;
  protected ArrayList<Integer> partitionKeys;


  public AbstractCouchBaseInputOperator()
  {
    store = new CouchBaseStore();
    partitionMask = 0;
    partitionKeys = new ArrayList<Integer>();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void emitTuples()
  {
      try {
        Object result = store.getInstance().getNumVBuckets();
                getVbucketByKey();
        T tuple = getTuple(result);
        outputPort.emit(tuple);
      }
      catch (Exception ex) {
        try {
          store.disconnect();
        }
        catch (IOException ex1) {
          DTThrowable.rethrow(ex1);
        }
        DTThrowable.rethrow(ex);
      }
  }

  public abstract T getTuple(Object object);

  public abstract List<String> getKeys();

  @Override
  public Collection<Partition<AbstractCouchBaseInputOperator<T>>> definePartitions(Collection<Partition<AbstractCouchBaseInputOperator<T>>> partitions, int incrementalCapacity)
  {
    List<String> keys = getKeys();
    int totalCount = partitions.size() + incrementalCapacity;
    int numPartitions = keys.size() % totalCount;
    Collection<Partition<AbstractCouchBaseInputOperator<T>>> newPartitions = Lists.newArrayListWithExpectedSize(numPartitions);
    Kryo kryo = new Kryo();
    for (int i = 0; i < numPartitions; i++) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      kryo.writeObject(output, this);
      output.close();
      Input lInput = new Input(bos.toByteArray());
      @SuppressWarnings("unchecked")
      AbstractCouchBaseInputOperator<T> oper = kryo.readObject(lInput, this.getClass());
      oper.setStore(this.store);
      newPartitions.add(new DefaultPartition<AbstractCouchBaseInputOperator<T>>(oper));
    }

    return newPartitions;

  }

  private void getVbucketByKey()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  public static class CouchBaseStreamCodec<T> extends KryoSerializableStreamCodec<T>
  {
    @Override
    public int getPartition(T tuple)
    {
      return tuple.hashCode();
    }

  }

}
