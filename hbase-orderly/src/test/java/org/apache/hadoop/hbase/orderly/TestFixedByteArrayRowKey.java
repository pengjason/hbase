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
import org.apache.hadoop.hbase.orderly.FixedByteArrayRowKey;
import org.apache.hadoop.hbase.orderly.RowKey;
import org.apache.hadoop.io.BytesWritable;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestFixedByteArrayRowKey extends TestFixedBytesWritableRowKey {

    final int length = 123;

    @Override
    public RowKey createRowKey() {
        return new FixedByteArrayRowKey(length);
    }

    @Override
    public Object createObject() {
        final byte[] randomBytes = new byte[length];
        r.nextBytes(randomBytes);
        return randomBytes;
    }

    @Override
    public int compareTo(Object o1, Object o2) {
        if (o1 == null || o2 == null)
            return (o1 != null ? 1 : 0) - (o2 != null ? 1 : 0);

        byte[] b1 = ((byte[])o1);
        byte[] b2 = ((byte[])o2);

        final int compareTo = new BytesWritable(b1).compareTo(new BytesWritable(b2));

        return compareTo < 0 ? -1 : compareTo > 0 ? 1 : 0;
    }
}
