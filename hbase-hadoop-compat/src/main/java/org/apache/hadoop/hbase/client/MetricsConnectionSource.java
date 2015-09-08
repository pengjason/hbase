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

public interface MetricsConnectionSource extends BaseSource {

  /** The name of the metrics. */
  String METRICS_NAME = "Connection";

  /** The name of the metrics context that metrics will be under. */
  String METRICS_CONTEXT = "connection";

  /** Description. */
  String METRICS_DESCRIPTION = "Metrics about HBase Connection";

  /**
   * The name of the metrics context that metrics will be under in jmx.
   *
   * The "sub01, sub02" stuff is how we create a nested bean structure. See explanation over on 
   * http://stackoverflow.com/questions/20669928/is-it-possible-to-create-jmx-subdomains
   */
  String METRICS_JMX_CONTEXT_FMT = "Client,sub01=%s,sub02=connection";

  /** Increment number of meta cache hits. */
  void incrMetaCacheHit();

  /** Increment number of meta cache misses. */
  void incrMetaCacheMiss();

  /** Add a new rpc call observation, duration of call in milliseconds. */
  void addRpcCallDuration(String method, String serverName, long ms);

  /** Add a new rpc call observation, request size in bytes. */
  void addRpcCallRequestSize(String method, String serverName, long bytes);

  /** Add a new rpc call observation, response size in bytes. */
  void addRpcCallResponseSize(String method, String serverName, long bytes);

  // Strings used for exporting to metrics system.
  String CONNECTION_ID_NAME = "connectionId";
  String CONNECTION_ID_DESC = "The connection's process-unique identifier.";
  String USER_NAME_NAME = "userName";
  String USER_NAME_DESC = "The user on behalf of whom the Connection is acting.";
  String CLUSTER_ID_NAME = "clusterId";
  String CLUSTER_ID_DESC = "Cluster Id";
  String ZOOKEEPER_QUORUM_NAME = "zookeeperQuorum";
  String ZOOKEEPER_QUORUM_DESC = "Zookeeper Quorum";
  String ZOOKEEPER_ZNODE_NAME = "zookeeperBaseZNode";
  String ZOOKEEPER_ZNODE_DESC = "Base ZNode for this cluster.";

  String META_CACHE_HIT_NAME = "metaCacheHit";
  String META_CACHE_HIT_DESC =
      "A counter on the number of times this connection's meta cache has a valid region location.";
  String META_CACHE_MISS_NAME = "metaCacheMiss";
  String META_CACHE_MISS_DESC =
      "A counter on the number of times this connection does not know where to find a region.";

  String META_LOOKUP_POOL_ACTIVE_THREAD_NAME = "metaLookupPoolActiveThreads";
  String META_LOOKUP_POOL_ACTIVE_THREAD_DESC =
      "The approximate number of threads actively resolving region locations from META.";
  String META_LOOKUP_POOL_LARGEST_SIZE_NAME = "metaLookupPoolLargestSize";
  String META_LOOKUP_POOL_LARGEST_SIZE_DESC =
      "The largest number of threads that have ever simultaneously been in the pool.";
  String EXEC_POOL_ACTIVE_THREAD_NAME = "executorPoolActiveThreads";
  String EXEC_POOL_ACTIVE_THREAD_DESC =
      "The approximate number of threads executing table operations.";
  String EXEC_POOL_LARGEST_SIZE_NAME = "executorPoolLargestSize";
  String EXEC_POOL_LARGEST_SIZE_DESC =
      "The largest number of threads that have ever simultaneously been in the pool.";

  String RPC_CALL_DURATION_NAME_BASE = "rpcCallDurationMs_";
  String RPC_CALL_DURATION_DESC = "The duration of an RPC call in milliseconds.";
  String RPC_CALL_REQUEST_SIZE_NAME_BASE = "rpcCallRequestSizeBytes_";
  String RPC_CALL_REQUEST_SIZE_DESC = "The size of an RPC call request in bytes.";
  String RPC_CALL_RESPONSE_SIZE_NAME_BASE = "rpcCallResponseSizeBytes_";
  String RPC_CALL_RESPONSE_SIZE_DESC = "The size of an RPC call response in bytes.";
}
