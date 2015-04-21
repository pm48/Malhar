package com.datatorrent.lib.dedup;

import com.datatorrent.lib.bucket.AbstractBucketManager;

/**
 * A {@link BucketManager} that creates buckets based on time.<br/>
 *
 * @since 0.9.4
 */
public class BucketManagerPOJOImpl extends AbstractBucketManager<SimpleEvent>
{
  @Override
  protected BucketPOJOImpl createBucket(long bucketKey)
  {
    return new BucketPOJOImpl(bucketKey);
  }

  @Override
  public long getBucketKeyFor(SimpleEvent event)
  {
    return Math.abs(event.getId()) / noOfBuckets;

  }

  /*
   * This method has been deprecated.Use clone instead.
   */
  @Deprecated
  @Override
  public BucketManagerPOJOImpl cloneWithProperties()
  {
    return null;
  }
}