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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.huahinframework.core.util.HadoopObject;
import org.huahinframework.core.util.ObjectUtil;
import org.huahinframework.core.util.PrimitiveObject;

/**
 * This abstract class is a class that holds the value.
 */
public abstract class AbstractWritable implements Writable {
    protected int order = 0;
    protected SortedMapWritable writableMap = new SortedMapWritable();

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        writableMap.readFields(in);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(DataOutput out) throws IOException {
        writableMap.write(out);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    /**
     * Clearing the retention value
     */
    public void clear() {
        writableMap.clear();
    }

    /**
     * @param label target label
     * @return The {@link Writable} value of the label. If it is not null.
     */
    @SuppressWarnings("rawtypes")
    public Writable getHadoopValue(String label) {
        for (Entry<WritableComparable, Writable> entry : writableMap.entrySet()) {
            Detail<?> kd = (Detail<?>) entry.getKey();
            if (kd.getLabel().equals(label)) {
                return entry.getValue();
            }
        }

        return null;
    }

    /**
     * @param label target label
     * @return The Java premitive value of the label. If it is not null.
     */
    @SuppressWarnings("rawtypes")
    public Object getPrimitiveValue(String label) {
        for (Entry<WritableComparable, Writable> entry : writableMap.entrySet()) {
            Detail<?> kd = (Detail<?>) entry.getKey();
            if (kd.getLabel().equals(label)) {
                return ObjectUtil.hadoop2Primitive(entry.getValue()).getObject();
            }
        }

        return null;
    }

    /**
     * @param label target label
     * @return The {@link HadoopObject} value of the label. If it is not null.
     */
    @SuppressWarnings("rawtypes")
    public HadoopObject getHadoopObject(String label) {
        for (Entry<WritableComparable, Writable> entry : writableMap.entrySet()) {
            Detail<?> kd = (Detail<?>) entry.getKey();
            if (kd.getLabel().equals(label)) {
                HadoopObject ho =
                        new HadoopObject(ObjectUtil.hadoop2Primitive(entry.getValue()).getType(),
                                         entry.getValue());
                return ho;
            }
        }

        return null;
    }

    /**
     * @param label target label
     * @return The {@link PrimitiveObject} value of the label. If it is not null.
     */
    @SuppressWarnings("rawtypes")
    public PrimitiveObject getPrimitiveObject(String label) {
        for (Entry<WritableComparable, Writable> entry : writableMap.entrySet()) {
            Detail<?> kd = (Detail<?>) entry.getKey();
            if (kd.getLabel().equals(label)) {
                return ObjectUtil.hadoop2Primitive(entry.getValue());
            }
        }

        return null;
    }
}
