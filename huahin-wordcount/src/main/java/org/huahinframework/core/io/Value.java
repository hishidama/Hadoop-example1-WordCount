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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.huahinframework.core.util.HadoopObject;
import org.huahinframework.core.util.ObjectUtil;
import org.huahinframework.core.util.StringUtil;

/**
 * This class is to set the Value of Hadoop.
 */
public class Value extends AbstractWritable {
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Value)) {
            return false;
        }

        Value other = (Value) obj;
        if (this.writableMap.size() != other.writableMap.size()) {
            return false;
        }

        return toString().equals(other.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Writable w : writableMap.values()) {
            if (w instanceof ArrayWritable) {
                ArrayWritable aw = (ArrayWritable) w;
                for (Writable x : aw.get()) {
                    sb.append(x.toString()).append(StringUtil.TAB);
                }
            } else if (w instanceof MapWritable) {
                MapWritable mw = (MapWritable) w;
                for (Entry<Writable, Writable> entry : mw.entrySet()){
                    sb.append(entry.getKey().toString().toString()).append(StringUtil.TAB)
                      .append(entry.getValue().toString().toString()).append(StringUtil.TAB);
                }
            } else {
                sb.append(w.toString()).append("\t");
            }
        }
        return sb.toString().substring(0, sb.toString().length() -1);
    }

    /**
     * Returns if true, values is nothing.
     * @return If true, values is nothing
     */
    public boolean isEmpty() {
        return writableMap.isEmpty();
    }

    /**
     * @param label value's label
     * @param writable add Hadoop Writable
     */
    public void addHadoopValue(String label, Writable writable) {
        if (writable == null) {
            writable = NullWritable.get();
        }

        writableMap.put(new ValueDetail(++order, label), writable);
    }

    /**
     * @param label value's label
     * @param object add Java primitive object
     */
    public void addPrimitiveValue(String label, Object object) {
        HadoopObject ho = ObjectUtil.primitive2Hadoop(object);
        if (ho.getObject() instanceof Writable) {
            writableMap.put(new ValueDetail(++order, label), ho.getObject());
            return;
        }

        throw new ClassCastException("object not WritableComparable");
    }
}
