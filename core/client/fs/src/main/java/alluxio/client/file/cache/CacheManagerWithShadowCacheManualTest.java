/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache;

import static java.util.concurrent.TimeUnit.SECONDS;

import alluxio.client.quota.CacheQuota;
import alluxio.client.quota.CacheScope;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.MetricRegistry;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

class DumpCacheManager implements CacheManager {

  @Override
  public boolean put(PageId pageId, byte[] page, CacheScope cacheScope, CacheQuota cacheQuota) {
    return false;
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer,
      int offsetInBuffer) {
    return 0;
  }

  @Override
  public boolean delete(PageId pageId) {
    return false;
  }

  @Override
  public State state() {
    return null;
  }

  @Override
  public void close() throws Exception {

  }
}


/**
 * A wrapper class of LocalCacheManager.
 */
public class CacheManagerWithShadowCacheManualTest {
  static DumpCacheManager kv = new DumpCacheManager();
  static CacheManagerWithShadowCache l;
  static int n = 0;

  public static void insert() {
    // The 20s working set is ~= 1M obj (40MB).
    // every second, insert 100k. And half of the working set is overlap with the previous second.
    byte[] bs = {0, 1, 2, 3};
    for (int i = n / 2; i < n / 2 + 100000; ++i) {
      PageId pid = new PageId("a", i);
      l.get(pid, 4, bs, 0);
      l.put(pid, bs);
    }
    n += 100000;
    MetricRegistry mr = MetricsSystem.METRIC_REGISTRY;
    int nPages = (int) mr.getCounters()
        .get(MetricsSystem.getMetricName(MetricKey.CLIENT_CACHE_SHADOW_CACHE_PAGES.getName()))
        .getCount();
    long nBytes = (long) mr.getCounters()
        .get(MetricsSystem.getMetricName(MetricKey.CLIENT_CACHE_SHADOW_CACHE_BYTES.getName()))
        .getCount();
    long nRead = (long) mr.getCounters()
        .get(MetricsSystem.getMetricName(MetricKey.CLIENT_CACHE_SHADOW_CACHE_PAGES_READ.getName()))
        .getCount();
    long nHit = (long) mr.getCounters()
        .get(MetricsSystem.getMetricName(MetricKey.CLIENT_CACHE_SHADOW_CACHE_PAGES_HIT.getName()))
        .getCount();
    double omr = nHit / (double) nRead;
    nRead = (long) mr.getCounters()
        .get(MetricsSystem.getMetricName(MetricKey.CLIENT_CACHE_SHADOW_CACHE_BYTES_READ.getName()))
        .getCount();
    nHit = (long) mr.getCounters()
        .get(MetricsSystem.getMetricName(MetricKey.CLIENT_CACHE_SHADOW_CACHE_BYTES_HIT.getName()))
        .getCount();
    double bmr = nHit / (double) nRead;
    System.out.printf("t=%d: %d inserted. Working set estimation: %d (%dB)%n",
        Instant.now().getEpochSecond(), n,
        nPages, nBytes);
    System.out.printf("bmr: %f, omr: %f%n", bmr, omr);
  }

  public static void main(String[] args) throws InterruptedException {
    InstancedConfiguration mConf = InstancedConfiguration.defaults();
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SHADOW_WINDOW, "20s");
    l = new CacheManagerWithShadowCache(kv, mConf);
    ScheduledExecutorService mScheduler = Executors.newScheduledThreadPool(0);
    mScheduler.scheduleAtFixedRate(() -> insert(), 0, 1, SECONDS);
  }
}
