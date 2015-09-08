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

import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.hadoop.metrics2.lib.MutableHistogram.MAX_METRIC_NAME;
import static org.apache.hadoop.metrics2.lib.MutableHistogram.MIN_METRIC_NAME;
import static org.apache.hadoop.metrics2.lib.MutableHistogram.NUM_OPS_METRIC_NAME;
import static org.apache.hadoop.hbase.client.MetricsConnectionSource.*;
import static org.junit.Assert.*;

/** Unit test over client metrics */
@Category({ ClientTests.class, SmallTests.class })
public class TestMetricsConnection {

  private static class MetricsConnectionWrapperStub implements MetricsConnectionWrapper {
    @Override public String getId() {
      return "testConnectionId";
    }

    @Override public String getUserName() {
      return "testUser";
    }

    @Override public String getClusterId() {
      return "testClusterId";
    }

    @Override public String getZookeeperQuorum() {
      return "foo:bar:baz";
    }

    @Override public String getZookeeperBaseNode() {
      return "/testing";
    }

    @Override public int getMetaLookupPoolActiveCount() {
      return 50;
    }

    @Override public int getMetaLookupPoolLargestPoolSize() {
      return 51;
    }

    @Override public int getExecutorPoolActiveCount() {
      return 52;
    }

    @Override public int getExecutorPoolLargestPoolSize() {
      return 53;
    }
  }

  public static MetricsAssertHelper HELPER =
      CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  private MetricsConnectionWrapper wrapper;
  private MetricsConnectionSourceImpl source;

  @BeforeClass
  public static void classSetUp() {
    HELPER.init();
  }

  @Before
  public void before() {
    wrapper = new MetricsConnectionWrapperStub();
    source = (MetricsConnectionSourceImpl) new MetricsConnection(wrapper).getMetricsSource();
  }

  @Test
  public void testWrapperSource() {
    HELPER.assertTag(CONNECTION_ID_NAME, wrapper.getId(), source);
    HELPER.assertTag(USER_NAME_NAME, wrapper.getUserName(), source);
    HELPER.assertTag(CLUSTER_ID_NAME, wrapper.getClusterId(), source);
    HELPER.assertTag(ZOOKEEPER_QUORUM_NAME, wrapper.getZookeeperQuorum(), source);
    HELPER.assertTag(ZOOKEEPER_ZNODE_NAME, wrapper.getZookeeperBaseNode(), source);
    HELPER.assertGauge(META_LOOKUP_POOL_ACTIVE_THREAD_NAME,
        wrapper.getMetaLookupPoolActiveCount(), source);
    HELPER.assertGauge(META_LOOKUP_POOL_LARGEST_SIZE_NAME,
        wrapper.getMetaLookupPoolLargestPoolSize(), source);
    HELPER.assertGauge(EXEC_POOL_ACTIVE_THREAD_NAME, wrapper.getExecutorPoolActiveCount(), source);
    HELPER.assertGauge(EXEC_POOL_LARGEST_SIZE_NAME, wrapper.getExecutorPoolLargestPoolSize(),
        source);
  }

  /** Should really be in a TestMetricsConnectionSourceImpl */
  @Test
  public void testDynamicMethodRegistration() {
    source.addRpcCallDuration("MethodFoo", "Server1", 10);
    source.addRpcCallDuration("MethodFoo", "Server1", 20);
    source.addRpcCallDuration("MethodFoo", "Server2", 30);
    source.addRpcCallDuration("MethodBar", "Server2", 40);

    HELPER.assertCounter(/* we should see 3 total calls to MethodFoo */
        RPC_CALL_DURATION_NAME_BASE + "MethodFoo" + NUM_OPS_METRIC_NAME, 3, source);
    HELPER.assertCounter(/* we should see 1 total call to MethodBar */
        RPC_CALL_DURATION_NAME_BASE + "MethodBar" + NUM_OPS_METRIC_NAME, 1, source);

    MetricsConnectionHostSource server1 = source.getOrCreate("Server1");
    assertNotNull("getOrCreate should always return something", server1);
    HELPER.assertCounter(/* 2 calls of MethodFoo were made to server1 */
        RPC_CALL_DURATION_NAME_BASE + "MethodFoo" + NUM_OPS_METRIC_NAME, 2, server1);
    HELPER.assertGauge(/* the smallest value reported by server1 should be 10 */
        RPC_CALL_DURATION_NAME_BASE + "MethodFoo" + MIN_METRIC_NAME, 10, server1);
    HELPER.assertGauge(/* the largest value reported by server1 should be 20 */
        RPC_CALL_DURATION_NAME_BASE + "MethodFoo" + MAX_METRIC_NAME, 20, server1);
    HELPER.assertCounterNotExist(/* assert no calls of MethodBar were made to server1 */
        RPC_CALL_DURATION_NAME_BASE + "MethodBar" + NUM_OPS_METRIC_NAME, server1);

    MetricsConnectionHostSource server2 = source.getOrCreate("Server2");
    assertNotNull("getOrCreate should always return something", server2);
    HELPER.assertCounter(/* 1 call of MethodFoo was made to server2 */
        RPC_CALL_DURATION_NAME_BASE + "MethodFoo" + NUM_OPS_METRIC_NAME, 1, server2);
    HELPER.assertGauge(/* the smallest value reported by server2 should be 30 */
        RPC_CALL_DURATION_NAME_BASE + "MethodFoo" + MIN_METRIC_NAME, 30, server2);
    HELPER.assertCounter(/* 1 call of MethodBar was made to server2 */
        RPC_CALL_DURATION_NAME_BASE + "MethodBar" + NUM_OPS_METRIC_NAME, 1, server2);
  }
}
