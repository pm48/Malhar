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
package com.datatorrent.lib.db.cache;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

/**
 * A {@link CacheManager.Primary} which keeps key/value pairs in memory.<br/>
 *
 * Properties of the cache store:<br/>
 * <ul>
 * <li>Transient: It is not checkpointed.</li>
 * <li>Max Cache Size: it starts evicting entries before this limit is exceeded.</li>
 * <li>Entry expiry time: the entries epire after the specified duration.</li>
 * <li>Cache cleanup interval: the interval at which the cache is cleaned up of expired entries periodically.</li>
 * </ul>
 * These properties of the cache are encapsulated in {@link CacheProperties}.<br/>
 *
 * @since 0.9.2
 */
public class CacheStore implements CacheManager.Primary
{
  @NotNull
  protected CacheProperties cacheProperties;

  private transient ScheduledExecutorService cleanupScheduler;
  private transient Cache<Object, Object> cache;
  private transient boolean open;

  public CacheStore()
  {
    cacheProperties = new CacheProperties();
  }

  @Override
  public void put(Object key, Object value)
  {
    cache.put(key, value);
  }

  @Override
  public Set<Object> getKeys()
  {
    return cache.asMap().keySet();
  }

  @Override
  public void putAll(Map<Object, Object> data)
  {
    cache.asMap().putAll(data);
  }

  @Override
  public void remove(Object key)
  {
    cache.invalidate(key);
  }

  @Override
  public Object get(Object key)
  {
    return cache.getIfPresent(key);
  }

  @Override
  public List<Object> getAll(List<Object> keys)
  {
    List<Object> values = Lists.newArrayList();
    for (Object key : keys) {
      values.add(cache.getIfPresent(key));
    }
    return values;
  }

  @Override
  public void connect() throws IOException
  {
    open = true;
    Preconditions.checkNotNull(cacheProperties.entryExpiryStrategy, "expiryType");

    CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
    if (cacheProperties.entryExpiryStrategy == ExpiryType.EXPIRE_AFTER_ACCESS) {
      cacheBuilder.expireAfterAccess(cacheProperties.entryExpiryDurationInMillis, TimeUnit.MILLISECONDS);
    }
    else if (cacheProperties.entryExpiryStrategy == ExpiryType.EXPIRE_AFTER_WRITE) {
      cacheBuilder.expireAfterWrite(cacheProperties.entryExpiryDurationInMillis, TimeUnit.MILLISECONDS);
    }
    cache = cacheBuilder.build();
    this.cleanupScheduler = Executors.newScheduledThreadPool(1);
    cleanupScheduler.scheduleAtFixedRate(new Runnable()
    {
      @Override
      public void run()
      {
        cache.cleanUp();
      }
    }, cacheProperties.cacheCleanupIntervalInMillis, cacheProperties.cacheCleanupIntervalInMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean isConnected()
  {
    return open;
  }

  @Override
  public void disconnect() throws IOException
  {
    open = false;
    cleanupScheduler.shutdown();
  }

  public void setCacheProperties(CacheProperties cacheProperties)
  {
    this.cacheProperties = cacheProperties;
  }

  public CacheProperties getCacheProperties()
  {
    return this.cacheProperties;
  }

  /**
   * Strategies for time-based expiration of entries.
   */
  public static enum ExpiryType
  {
    /**
     * Only expire the entries after the specified duration has passed since the entry was last accessed by a read
     * or a write.
     */
    EXPIRE_AFTER_ACCESS,
    /**
     * Expire the entries after the specified duration has passed since the entry was created, or the most recent
     * replacement of the value.
     */
    EXPIRE_AFTER_WRITE
  }
}
