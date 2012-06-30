/*
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
package org.huahinframework.core.lib.partition;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.huahinframework.core.io.Key;

/**
 * <code>SimpleGroupingComparator</code> grouping key is that you specify in the <code>Record#addGrouping</code>.
 */
public class SimpleGroupingComparator extends WritableComparator {
    /**
     * default constractor
     */
    public SimpleGroupingComparator() {
        super(Key.class, true);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof Key && b instanceof Key) {
            Comparable one = Key.class.cast(a).identifier();
            Comparable other = Key.class.cast(b).identifier();

            return one.compareTo(other);
        }

        return super.compare(a, b);
    }
}
