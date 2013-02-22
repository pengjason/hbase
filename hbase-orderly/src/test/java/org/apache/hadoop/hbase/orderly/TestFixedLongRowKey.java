/*  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.hadoop.hbase.orderly;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.io.LongWritable;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestFixedLongRowKey extends TestFixedLongWritableRowKey
{
  @Override
  public RowKey createRowKey() { return new FixedLongRowKey(); }

  @Override
  public Object createObject() {
    return Long.valueOf(((LongWritable)super.createObject()).get());
  }

  @Override
  public int compareTo(Object o1, Object o2) {
    long x = (Long) o1,
         y = (Long) o2;

    return ((x > y) ? 1 : 0) - ((y > x) ? 1 : 0);
  }
}
