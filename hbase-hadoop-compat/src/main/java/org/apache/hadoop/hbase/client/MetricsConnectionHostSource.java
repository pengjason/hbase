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

import org.apache.hadoop.hbase.metrics.BaseSource;

/**
 * Reports Host-level metrics as observed by the client.
 */
public interface MetricsConnectionHostSource extends BaseSource {
  /** The name of the metrics. */
  String METRICS_NAME = "Host";

  /** The name of the metrics context that metrics will be under. */
  String METRICS_CONTEXT = "connection";

  /** Description */
  String METRICS_DESCRIPTION = "Metrics describing interactions with target host";

  /** The name of the metrics context that metrics will be under in jmx. */
  String METRICS_JMX_CONTEXT_FMT = "Client,sub01=%s,sub02=%s";

  /** Prefix string used for describing the rpc call time for a specific method. */
  String RPC_CALL_DURATION_NAME_BASE = "rpcCallDurationMs_";
  String RPC_CALL_DURATION_DESC = "The duration of an RPC call in milliseconds.";
  /** Prefix string used for describing the rpc call time for a specific method. */
  String RPC_CALL_REQUEST_SIZE_NAME_BASE = "rpcCallRequestSizeBytes_";
  String RPC_CALL_REQUEST_SIZE_DESC = "The size of an RPC call request in bytes.";
  /** Prefix string used for describing the rpc call time for a specific method. */
  String RPC_CALL_RESPONSE_SIZE_NAME_BASE = "rpcCallResponseSizeBytes_";
  String RPC_CALL_RESPONSE_SIZE_DESC = "The size of an RPC call response in bytes.";

  /** Record an RPC call duration. */
  void addRpcCallDuration(String method, long ms);

  /** Record an RPC request size. */
  void addRpcCallRequestSize(String method, long bytes);

  /** Record an RPC response size. */
  void addRpcCallResponseSize(String method, long bytes);
}
