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

package org.apache.hadoop.hbase.metrics;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.impl.JmxCacheBuster;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MetricMutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.hadoop.metrics2.source.JvmMetrics;

/**
 * Hadoop 2 implementation of BaseSource (using metrics2 framework).  It handles registration to
 * DefaultMetricsSystem and creation of the metrics registry.
 *
 * All MetricsSource's in hbase-hadoop2-compat should derive from this class.
 */
@InterfaceAudience.Private
public class BaseSourceImpl implements BaseSource, MetricsSource {

  private static enum DefaultMetricsSystemInitializer {
    INSTANCE;
    private boolean inited = false;

    synchronized void init(String name) {
      if (inited) return;
      inited = true;
      DefaultMetricsSystem.initialize(HBASE_METRICS_SYSTEM_NAME);
      JvmMetrics.initSingleton(name, "");
    }
  }

  protected final DynamicMetricsRegistry metricsRegistry;
  protected final String metricsName;
  protected final String metricsDescription;
  protected final String metricsContext;
  protected final String metricsJmxContext;

  public BaseSourceImpl(
      String metricsName,
      String metricsDescription,
      String metricsContext,
      String metricsJmxContext) {

    this.metricsName = metricsName;
    this.metricsDescription = metricsDescription;
    this.metricsContext = metricsContext;
    this.metricsJmxContext = metricsJmxContext;

    metricsRegistry = new DynamicMetricsRegistry(metricsName).setContext(metricsContext);
    DefaultMetricsSystemInitializer.INSTANCE.init(metricsName);

    //Register this instance.
    DefaultMetricsSystem.instance().register(metricsJmxContext, metricsDescription, this);
    init();

  }

  /**
   * Dynamically register a histogram metrics when it does not exist. Does right by descriptions,
   * but probably generates a bit of unnecessary garbage while the registry is populated the first
   * time.
   */
  protected MutableHistogram getHistogram(String name, String description) {
    if (getMetricsRegistry().get(name) == null) {
      getMetricsRegistry().newHistogram(name, description);
    }
    return getMetricsRegistry().getHistogram(name);
  }

  @Override
  public void init() {
    this.metricsRegistry.clearMetrics();
  }

  @Override
  public void setGauge(String gaugeName, long value) {
    MutableGaugeLong gaugeInt = metricsRegistry.getLongGauge(gaugeName, value);
    gaugeInt.set(value);
  }

  @Override
  public void incGauge(String gaugeName, long delta) {
    MutableGaugeLong gaugeInt = metricsRegistry.getLongGauge(gaugeName, 0l);
    gaugeInt.incr(delta);
  }

  @Override
  public void decGauge(String gaugeName, long delta) {
    MutableGaugeLong gaugeInt = metricsRegistry.getLongGauge(gaugeName, 0l);
    gaugeInt.decr(delta);
  }

  @Override
  public void incCounters(String key, long delta) {
    MutableCounterLong counter = metricsRegistry.getLongCounter(key, 0l);
    counter.incr(delta);

  }

  @Override
  public void updateHistogram(String name, long value) {
    MutableHistogram histo = metricsRegistry.getHistogram(name);
    histo.add(value);
  }

  @Override
  public void updateQuantile(String name, long value) {
    MetricMutableQuantiles histo = metricsRegistry.getQuantile(name);
    histo.add(value);
  }

  @Override
  public void removeMetric(String key) {
    metricsRegistry.removeMetric(key);
    JmxCacheBuster.clearJmxCache();
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    metricsRegistry.snapshot(metricsCollector.addRecord(metricsRegistry.info()), all);
  }

  public DynamicMetricsRegistry getMetricsRegistry() {
    return metricsRegistry;
  }

  @Override
  public String getMetricsContext() {
    return metricsContext;
  }

  @Override
  public String getMetricsDescription() {
    return metricsDescription;
  }

  @Override
  public String getMetricsJmxContext() {
    return metricsJmxContext;
  }

  @Override
  public String getMetricsName() {
    return metricsName;
  }

}
