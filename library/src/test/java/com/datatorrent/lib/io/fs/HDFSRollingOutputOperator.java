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

import com.datatorrent.api.Context.OperatorContext;
import java.io.File;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;

public class HDFSRollingOutputOperator<T> extends AbstractFSWriter<T>
  {
    private transient String outputFileName;
    protected MutableInt partNumber;
    protected MutableLong offset;
    protected String lastFile;
    protected HiveInsertOperator hive;
   // private  final Logger logger = LoggerFactory.getLogger(HDFSRollingOutputOperator.class);
    public HDFSRollingOutputOperator()
    {
      setMaxLength(128);
    }

    @Override
    public void setup(OperatorContext context)
    {
      outputFileName = File.separator + "transactions.out.part";
      super.setup(context);
    }

    @Override
    protected void rotateHook(String finishedFile)
    {
      hive.filenames.put(finishedFile, hive.windowIDOfCompletedPart);
    }


    @Override
    protected String getFileName(T tuple)
    {
      return outputFileName;
    }

    @Override
    protected byte[] getBytesForTuple(T tuple)
    {
      String hiveTuple = hive.getHiveTuple(tuple);
      return hiveTuple.getBytes();
    }
  }