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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;

/**
 * Reports Host-level metrics as observed by the client.
 */
@InterfaceAudience.Private
public class MetricsConnectionHostSourceImpl extends BaseSourceImpl
    implements MetricsConnectionHostSource {

  public MetricsConnectionHostSourceImpl(MetricsConnectionWrapper wrapper, String serverName) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT,
        String.format(METRICS_JMX_CONTEXT_FMT, wrapper.getId(), serverName));
  }

  public MetricsConnectionHostSourceImpl(String metricsName, String metricsDescription,
      String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  @Override public void addRpcCallDuration(String method, long ms) {
    getHistogram(RPC_CALL_DURATION_NAME_BASE + method, RPC_CALL_DURATION_DESC).add(ms);
  }

  @Override public void addRpcCallRequestSize(String method, long bytes) {
    getHistogram(RPC_CALL_REQUEST_SIZE_NAME_BASE + method, RPC_CALL_REQUEST_SIZE_DESC)
        .add(bytes);
  }

  @Override public void addRpcCallResponseSize(String method, long bytes) {
    getHistogram(RPC_CALL_RESPONSE_SIZE_NAME_BASE + method, RPC_CALL_RESPONSE_SIZE_DESC)
        .add(bytes);
  }
}