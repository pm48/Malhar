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
package com.datatorrent.lib.pigquery;


/**
 * This operator semantic maps to stream operators already in library.  <br>
 * <p>
 * Please check following operators : <br>
 * {@link com.datatorrent.lib.script.BashOperator}. <br>
 * {@link com.datatorrent.lib.script.JavaScriptOperator}. <br>
 * {@link com.datatorrent.lib.script.PythonOperator}.  <br>
 * @displayName Pig Stream
 * @category Pig Query
 * @tags script, stream operator
 * @since 0.3.4
 */
@Deprecated
public class PigStreamOperator
{
  // must not be null.   
  private PigStreamOperator() 
  {
    assert(false);
  }
}
