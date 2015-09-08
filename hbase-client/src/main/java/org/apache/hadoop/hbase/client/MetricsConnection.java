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
import com.google.protobuf.Descriptors;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * This class is for maintaining the various connection statistics and publishing them through
 * the metrics interfaces.
 */
@InterfaceAudience.Private
public class MetricsConnection {

  /** A container class for collecting details about the RPC call as it percolates. */
  public static class CallStats {
    private long requestSizeBytes = 0;
    private long responseSizeBytes = 0;
    private long startTime = 0;
    private long callTimeMs = 0;

    public long getRequestSizeBytes() {
      return requestSizeBytes;
    }

    public void setRequestSizeBytes(long requestSizeBytes) {
      this.requestSizeBytes = requestSizeBytes;
    }

    public long getResponseSizeBytes() {
      return responseSizeBytes;
    }

    public void setResponseSizeBytes(long responseSizeBytes) {
      this.responseSizeBytes = responseSizeBytes;
    }

    public long getStartTime() {
      return startTime;
    }

    public void setStartTime(long startTime) {
      this.startTime = startTime;
    }

    public long getCallTimeMs() {
      return callTimeMs;
    }

    public void setCallTimeMs(long callTimeMs) {
      this.callTimeMs = callTimeMs;
    }
  }

  private final MetricsConnectionWrapper wrapper;
  private final MetricsConnectionSource source;

  public MetricsConnection(MetricsConnectionWrapper wrapper) {
    this.wrapper = wrapper;
    this.source = CompatibilitySingletonFactory.getInstance(MetricsConnectionSourceFactory.class)
        .createConnection(this.wrapper);
  }

  @VisibleForTesting
  MetricsConnectionSource getMetricsSource() {
    return source;
  }

  /** Produce an instance of {@link CallStats} for clients to attach to RPCs. */
  public static CallStats newCallStats() {
    // TODO: instance pool to reduce GC?
    return new CallStats();
  }

  /** Increment the number of meta cache hits. */
  public void incrMetaCacheHit() {
    source.incrMetaCacheHit();
  }

  /** Increment the number of meta cache misses. */
  public void incrMetaCacheMiss() {
    source.incrMetaCacheMiss();
  }

  /** Report RPC context to metrics system. */
  public void updateRpc(Descriptors.MethodDescriptor method, ServerName serverName,
      CallStats stats) {
    final String methodName = method.getService().getName() + "_" + method.getName();
    final String sn = serverName.toShortString().replace(':', '-');
    source.addRpcCallDuration(methodName, sn, stats.callTimeMs);
    source.addRpcCallRequestSize(methodName, sn, stats.requestSizeBytes);
    source.addRpcCallResponseSize(methodName, sn, stats.responseSizeBytes);
  }
}
