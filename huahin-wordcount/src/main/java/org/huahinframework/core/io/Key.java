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
package org.huahinframework.core.io;

import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.huahinframework.core.util.HadoopObject;
import org.huahinframework.core.util.ObjectUtil;
import org.huahinframework.core.util.StringUtil;

/**
 * This class is to set the Key of Hadoop.
 */
public class Key extends AbstractWritable implements WritableComparable<Key> {
    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(Key key) {
        Text one = (Text) this.identifier();
        Text other = (Text) key.identifier();
        return one.compareTo(other);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Key)) {
            return false;
        }

        Key other = (Key) obj;
        if (this.writableMap.size() != other.writableMap.size()) {
            return false;
        }

        return toString().equals(other.toString());
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Entry<WritableComparable, Writable> entry : writableMap.entrySet()) {
            KeyDetail kd = (KeyDetail) entry.getKey();
            if (kd.getGrouping()) {
                sb.append(entry.getValue().toString()).append(StringUtil.TAB);
            }
        }

        if (sb.length() == 0) {
            return "";
        }

        return sb.toString().substring(0, sb.toString().length() - 1);
    }

    /**
     * Returns a value that is used for grouping
     * @return grouping values
     */
    public WritableComparable<?> identifier() {
        return new Text(toString());
    }

    /**
     * Returns the Key for sorting.
     * @return value of sort
     */
    @SuppressWarnings("rawtypes")
    public SortedMapWritable sort() {
        SortedMapWritable smw = new SortedMapWritable();
        for (Entry<WritableComparable, Writable> entry : writableMap.entrySet()) {
            KeyDetail kd = (KeyDetail) entry.getKey();
            if (kd.getSort() != Record.SORT_NON) {
                kd.setKey((WritableComparable) entry.getValue());
                smw.put(new IntWritable(kd.getSortPriority()), kd);
            }
        }

        return smw;
    }

    /**
     * set new sort
     * @param smw sort
     */
    @SuppressWarnings("rawtypes")
    public void setSort(SortedMapWritable smw) {
        for (Entry<WritableComparable, Writable> entry : smw.entrySet()) {
            KeyDetail kd = (KeyDetail) entry.getKey();
            kd.setOrder(++order);
            writableMap.put(kd, entry.getValue());
        }
    }

    /**
     * Returns a value that is used for sort
     * @return sort
     */
    @SuppressWarnings("rawtypes")
    public SortedMapWritable getSort() {
        SortedMapWritable smw = new SortedMapWritable();
        for (Entry<WritableComparable, Writable> entry : writableMap.entrySet()) {
            KeyDetail kd = (KeyDetail) entry.getKey();
            if (kd.getSort() != Record.SORT_NON) {
                smw.put(kd, entry.getValue());
            }
        }

        return smw;
    }

    /**
     * set new grouping
     * @param smw grouping
     */
    @SuppressWarnings("rawtypes")
    public void setGrouping(SortedMapWritable smw) {
        for (Entry<WritableComparable, Writable> entry : smw.entrySet()) {
            KeyDetail kd = (KeyDetail) entry.getKey();
            addHadoopValue(kd.getLabel(), (WritableComparable<?>) entry.getValue());
        }
    }

    /**
     * Returns a value that is used for grouping
     * @return grouping
     */
    @SuppressWarnings("rawtypes")
    public SortedMapWritable getGrouping() {
        SortedMapWritable smw = new SortedMapWritable();
        for (Entry<WritableComparable, Writable> entry : writableMap.entrySet()) {
            KeyDetail kd = (KeyDetail) entry.getKey();
            if (kd.getGrouping()) {
                smw.put(kd, entry.getValue());
            }
        }

        return smw;
    }

    /**
     * @param label value's label
     * @param writable add Hadoop Writable
     */
    public void addHadoopValue(String label, WritableComparable<?> writable) {
        addHadoopValue(label, writable, true, Record.SORT_NON, 0);
    }

    /**
     * Returns if true, values is nothing.
     * @return If true, values is nothing
     */
    public boolean isEmpty() {
        return writableMap.isEmpty();
    }

    /**
     * Returns if true, grouping is nothing.
     * @return If true, grouping is nothing
     */
    public boolean isGroupingEmpty() {
        return getGrouping().isEmpty();
    }

    /**
     * Returns if true, sort is nothing.
     * @return If true, sort is nothing
     */
    public boolean isSortEmpty() {
        return getSort().isEmpty();
    }

    /**
     * @param label value's label
     * @param writable add Hadoop Writable
     * @param sort If true, set the sort
     * @param sortPriority the sort order
     */
    public void addHadoopValue(String label, WritableComparable<?> writable, int sort, int sortPriority) {
        if (writable == null) {
            writable = NullWritable.get();
        }

        writableMap.put(new KeyDetail(++order, label, false, sort, sortPriority), writable);
    }

    /**
     * @param label value's label
     * @param writable add Hadoop Writable
     * @param grouping If true, set the grouping
     * @param sort If true, set the sort
     * @param sortPriority the sort order
     */
    public void addHadoopValue(String label, WritableComparable<?> writable, boolean grouping, int sort, int sortPriority) {
        if (writable == null) {
            writable = NullWritable.get();
        }

        writableMap.put(new KeyDetail(++order, label, grouping, sort, sortPriority), writable);
    }

    /**
     * @param label value's label
     * @param object add Java primitive object
     */
    public void addPrimitiveValue(String label, Object object) {
        addPrimitiveValue(label, object, true, Record.SORT_NON, 0);
    }

    /**
     * @param label value's label
     * @param object add Java primitive object
     * @param sort If true, set the sort
     * @param sortPriority the sort order
     */
    public void addPrimitiveValue(String label, Object object, int sort, int sortPriority) {
        HadoopObject ho = ObjectUtil.primitive2Hadoop(object);
        if (ho.getObject() instanceof WritableComparable) {
            writableMap.put(new KeyDetail(++order, label, false, sort, sortPriority), ho.getObject());
            return;
        }

        throw new ClassCastException("object not WritableComparable");
    }

    /**
     * @param label value's label
     * @param object add Java primitive object
     * @param grouping If true, set the grouping
     * @param sort If true, set the sort
     * @param sortPriority the sort order
     */
    public void addPrimitiveValue(String label, Object object, boolean grouping, int sort, int sortPriority) {
        HadoopObject ho = ObjectUtil.primitive2Hadoop(object);
        if (ho.getObject() instanceof WritableComparable) {
            writableMap.put(new KeyDetail(++order, label, grouping, sort, sortPriority), ho.getObject());
            return;
        }

        throw new ClassCastException("object not WritableComparable");
    }
}
