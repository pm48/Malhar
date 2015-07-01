/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.util;

import com.datatorrent.common.util.BaseOperator;

/**
 * This is an abstract operator that allows cloneKey to allow users to use mutable objects.
 * <p></p>
 * @displayName Base Key
 * @category Algorithmic
 * @tags abstract, cloneKey
 * @since 0.3.2
 */
public class BaseKeyOperator<K> extends BaseOperator
{
  /**
   * Override this call in case you have mutable objects. By default the objects are assumed to be immutable
   * @param k to be cloned
   * @return k as is
   */
  public K cloneKey(K k)
  {
    return k;
  }
}
