/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import java.util.HashMap;
import java.util.Map;

@InterfaceAudience.Private
public class MetricsConnectionSourceImpl
    extends BaseSourceImpl implements MetricsConnectionSource {

  // wrapper for access statistics collected in Connection instance
  private final MetricsConnectionWrapper wrapper;

  // Hadoop Metric2 objects for additional monitoring

  private final MutableCounterLong metaCacheHit;
  private final MutableCounterLong metaCacheMiss;

  private final Map<String, MetricsConnectionHostSource> byHost;

  public MetricsConnectionSourceImpl(MetricsConnectionWrapper wrapper) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT,
        String.format(METRICS_JMX_CONTEXT_FMT, wrapper.getId()), wrapper);
  }

  public MetricsConnectionSourceImpl(String metricsName, String metricsDescription,
      String metricsContext, String metricsJmxContext, MetricsConnectionWrapper wrapper) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.wrapper = wrapper;

    metaCacheHit = getMetricsRegistry().newCounter(META_CACHE_HIT_NAME, META_CACHE_HIT_DESC, 0L);
    metaCacheMiss =
        getMetricsRegistry().newCounter(META_CACHE_MISS_NAME, META_CACHE_MISS_DESC, 0L);
    byHost = new HashMap<>();
  }

  /**
   * Dynamically register metrics on first invocation. A new bean is registered for
   * {@code serverName} upon initial encounter. RPC calls to said host are registered on that bean
   * on first invocation.
   */
  @VisibleForTesting
  MetricsConnectionHostSource getOrCreate(String serverName) {
    // TODO: how to handle expiration of metrics for transient hosts?
    MetricsConnectionHostSource hostSource = byHost.get(serverName);
    if (hostSource == null) {
      synchronized (byHost) {
        hostSource = byHost.get(serverName);
        if (hostSource == null) {
          hostSource = new MetricsConnectionHostSourceImpl(wrapper, serverName);
          byHost.put(serverName, hostSource);
        }
      }
    }
    return hostSource;
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {

    MetricsRecordBuilder mrb = metricsCollector.addRecord(metricsName);

    if (wrapper != null) {
      mrb.addGauge(Interns.info(META_LOOKUP_POOL_LARGEST_SIZE_NAME,
          META_LOOKUP_POOL_LARGEST_SIZE_DESC), wrapper.getMetaLookupPoolLargestPoolSize())
          .addGauge(Interns.info(META_LOOKUP_POOL_ACTIVE_THREAD_NAME,
              META_LOOKUP_POOL_ACTIVE_THREAD_DESC), wrapper.getMetaLookupPoolActiveCount())
          .addGauge(Interns.info(EXEC_POOL_ACTIVE_THREAD_NAME, EXEC_POOL_ACTIVE_THREAD_DESC),
              wrapper.getExecutorPoolActiveCount())
          .addGauge(Interns.info(EXEC_POOL_LARGEST_SIZE_NAME, EXEC_POOL_LARGEST_SIZE_DESC),
              wrapper.getExecutorPoolLargestPoolSize())
          .tag(Interns.info(CONNECTION_ID_NAME, CONNECTION_ID_DESC), wrapper.getId())
          .tag(Interns.info(USER_NAME_NAME, USER_NAME_DESC), wrapper.getUserName())
          .tag(Interns.info(CLUSTER_ID_NAME, CLUSTER_ID_DESC), wrapper.getClusterId())
          .tag(Interns.info(ZOOKEEPER_QUORUM_NAME, ZOOKEEPER_QUORUM_DESC),
              wrapper.getZookeeperQuorum())
          .tag(Interns.info(ZOOKEEPER_ZNODE_NAME, ZOOKEEPER_ZNODE_DESC),
              wrapper.getZookeeperBaseNode());
    }

    metricsRegistry.snapshot(mrb, all);
  }

  @Override public void incrMetaCacheHit() {
    metaCacheHit.incr();
  }

  @Override public void incrMetaCacheMiss() {
    metaCacheMiss.incr();
  }

  @Override public void addRpcCallDuration(String method, String serverName, long ms) {
    getHistogram(RPC_CALL_DURATION_NAME_BASE + method, RPC_CALL_DURATION_DESC).add(ms);
    getOrCreate(serverName).addRpcCallDuration(method, ms);
  }

  @Override public void addRpcCallRequestSize(String method, String serverName, long bytes) {
    getHistogram(RPC_CALL_REQUEST_SIZE_NAME_BASE + method, RPC_CALL_REQUEST_SIZE_DESC).add(bytes);
    getOrCreate(serverName).addRpcCallRequestSize(method, bytes);
  }

  @Override public void addRpcCallResponseSize(String method, String serverName, long bytes) {
    getHistogram(RPC_CALL_RESPONSE_SIZE_NAME_BASE + method, RPC_CALL_RESPONSE_SIZE_DESC)
        .add(bytes);
    getOrCreate(serverName).addRpcCallResponseSize(method, bytes);
  }
}
