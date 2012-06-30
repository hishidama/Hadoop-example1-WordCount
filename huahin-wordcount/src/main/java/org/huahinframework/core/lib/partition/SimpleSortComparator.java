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

import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.huahinframework.core.io.Key;
import org.huahinframework.core.io.KeyDetail;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.util.ObjectUtil;

/**
 * <code>SimpleSortComparator</code> is sorted by keys that are specified in the <code>Record#addSort</code>.
 */
public class SimpleSortComparator extends WritableComparator {
    /**
     * default constractor
     */
    public SimpleSortComparator() {
        super(Key.class, true);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof Key && b instanceof Key) {
            Comparable oneIdentifier = Key.class.cast(a).identifier();
            Comparable otherIdentifier = Key.class.cast(b).identifier();

            int identifier = oneIdentifier.compareTo(otherIdentifier);
            if (identifier != 0) {
                return identifier;
            }

            SortedMapWritable oneSort = Key.class.cast(a).sort();
            SortedMapWritable otherSort = Key.class.cast(b).sort();
            if (oneSort.size() != otherSort.size()) {
                return -1;
            }

            for (Entry<WritableComparable, Writable> entry : oneSort.entrySet()) {
                IntWritable priority = ((IntWritable) entry.getKey());
                KeyDetail oneKeyDetail = (KeyDetail) entry.getValue();
                KeyDetail otherKeyDetail = (KeyDetail) otherSort.get(priority);

                WritableComparable oneKey = oneKeyDetail.getKey();
                WritableComparable otherKey = otherKeyDetail.getKey();
                if (ObjectUtil.typeCompareTo(oneKeyDetail.getKey(), otherKeyDetail.getKey()) != 0) {
                    return -1;
                }

                int cmpare = oneKey.compareTo(otherKey);
                if (cmpare != 0) {
                    if (oneKeyDetail.getSort() == Record.SORT_LOWER) {
                        return cmpare;
                    } else {
                        return -cmpare;
                    }
                }
            }

            return 0;
        }

        return super.compare(a, b);
    }
}
