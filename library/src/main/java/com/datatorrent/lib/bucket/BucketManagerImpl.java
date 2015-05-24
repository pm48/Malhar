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
package com.datatorrent.lib.bucket;

 /**
  * <p>
  * A {@link AbstractBucketManager} implementation.
  * </p>
  *
  * @since 2.1.0
  */
public class BucketManagerImpl<T extends Bucketable> extends AbstractBucketManager<T>
{

  @Override
  protected Bucket<T> createBucket(long bucketKey)
  {
    return new Bucket<T>(bucketKey);
  }

  @Override
  public long getBucketKeyFor(T event)
  {
    return Math.abs(event.getEventKey().hashCode()) / noOfBuckets;
  }

  /*
   * This method has been deprecated.Use clone instead.
   */
  @Deprecated
  @Override
  public BucketManagerImpl<T> cloneWithProperties()
  {
    return null;
  }

}
