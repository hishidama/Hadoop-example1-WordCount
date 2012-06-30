/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the oTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may ot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WIToUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.huahinframework.core.io;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.huahinframework.core.util.HadoopObject;
import org.huahinframework.core.util.ObjectUtil;
import org.huahinframework.core.util.PrimitiveObject;

/**
 * <code>Record</code> is treated as one record and Value Key.
 *
 * <p>Can set there are three types of grouping, sorting, and value.
 * In addition, the acquisition of value can be retrieved by specifying a Java primitive value and Hadoop {@link Writable}.</p>
 *
 * <p>The following example retrieves the value from the Record.</p>
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public void filter(Record record, Writer writer) {
 *   String url = record.getGroupingString("URL");
 *   String user = record.getValueString("USER");
 *   int pv = record.getValueInteger("PV");
 * }
 * </pre></blockquote></p>
 *
 * <p>The following example sets a value in the Record.</p>
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public void filter(Record record, Writer writer) {
 *   Record emitRecord = new Record();
 *   emitRecord.addGrouping("URL", url);
 *   emitRecord.addSort(pv, Record.SORT_UPPER, 1);
 *   emitRecord.addValue("USER", user);
 *   emitRecord.addValue("PV", pv);
 * }
 * </pre></blockquote></p>
 */
public class Record {
    /**
     * sort nothing
     */
    public static final int SORT_NON = 0;

    /**
     * sort order lower
     */
    public static final int SORT_LOWER = 1;

    /**
     * sort order upper
     */
    public static final int SORT_UPPER = 2;

    private static final int KEY = 1;
    private static final int VALUE = 2;

    private static final String SORT_LABEL = "SORT_%d";

    private Key key;
    private Value value;

    private boolean groupingNothing = false;
    private boolean valueNothing = false;

    /**
     * default constractor
     */
    public Record() {
        key = new Key();
        value = new Value();
    }

    /**
     * @param key Key
     * @param value Value
     */
    public Record(Key key, Value value) {
        this.key = key;
        this.value = value;
    }

    /**
     * @return the key
     */
    public Key getKey() {
        return key;
    }

    /**
     * @param key the key to set
     */
    public void setKey(Key key) {
        this.key = key;
    }

    /**
     * @return the value
     */
    public Value getValue() {
        return value;
    }

    /**
     * Returns if true, Key is nothing.
     * @return if true, Key is nothing.
     */
    public boolean isKeyEmpty() {
        return key.isEmpty();
    }

    /**
     * Returns if true, Value is nothing.
     * @return if true, Value is nothing
     */
    public boolean isValueEmpty() {
        return value.isEmpty();
    }

    /**
     * @return the groupingNothing
     */
    public boolean isGroupingNothing() {
        return groupingNothing;
    }

    /**
     * @param groupingNothing the groupingNothing to set
     */
    public void setGroupingNothing(boolean groupingNothing) {
        this.groupingNothing = groupingNothing;
    }

    /**
     * @return the valueNothing
     */
    public boolean isValueNothing() {
        return valueNothing;
    }

    /**
     * @param valueNothing the valueNothing to set
     */
    public void setValueNothing(boolean valueNothing) {
        this.valueNothing = valueNothing;
    }

    /**
     * @param value the value to set
     */
    public void setValue(Value value) {
        this.value = value;
    }

    /**
     * Add grouping value
     * @param label grouping label
     * @param object grouping Java primitive value
     */
    public void addGrouping(String label, Object object) {
        key.addPrimitiveValue(label, object);
    }

    /**
     * Add grouping value
     * @param label grouping label
     * @param writable grouping Hadoop value
     */
    public void addGrouping(String label, WritableComparable<?> writable) {
        key.addHadoopValue(label, writable);
    }

    /**
     * Add sort key
     * @param object Java primitive sort key
     * @param sort sort type SORT_NON or SORT_LOWER or SORT_UPPER
     * @param priority sort order
     */
    public void addSort(Object object, int sort, int priority) {
        key.addPrimitiveValue(String.format(SORT_LABEL, priority), object, sort, priority);
    }

    /**
     * Add sort key
     * @param writable Hadoop sort key
     * @param sort sort type SORT_NON or SORT_LOWER or SORT_UPPER
     * @param priority sort order
     */
    public void addSort(WritableComparable<?> writable, int sort, int priority) {
        key.addHadoopValue(String.format(SORT_LABEL, priority), writable, sort, priority);
    }

    /**
     * Add value
     * @param label value's label
     * @param object Java primitive value
     */
    public void addValue(String label, Object object) {
        value.addPrimitiveValue(label, object);
    }

    /**
     * Add value
     * @param label value's label
     * @param writable Hadoop value
     */
    public void addValue(String label, Writable writable) {
        value.addHadoopValue(label, writable);
    }

    /**
     * Get grouping {@link String} value
     * @param label target label
     * @return {@link String} value of the label. If it is not null.
     */
    public String getGroupingString(String label) {
        PrimitiveObject o = getPrimitiveObject(KEY, label, ObjectUtil.STRING, "String");
        if (o == null) {
            return null;
        }
        return (String) o.getObject();
    }

    /**
     * Get grouping {@link Byte} value
     * @param label target label
     * @return {@link Byte} value of the label. If it is not null.
     */
    public Byte getGroupingByte(String label) {
        PrimitiveObject o = getPrimitiveObject(KEY, label, ObjectUtil.BYTE, "Byte");
        if (o == null) {
            return null;
        }
        return (Byte) o.getObject();
    }

    /**
     * Get grouping {@link Integer} value
     * @param label target label
     * @return {@link Integer} value of the label. If it is not null.
     */
    public Integer getGroupingInteger(String label) {
        PrimitiveObject o = getPrimitiveObject(KEY, label, ObjectUtil.INTEGER, "Integer");
        if (o == null) {
            return null;
        }
        return (Integer) o.getObject();
    }

    /**
     * Get grouping {@link Long} value
     * @param label target label
     * @return {@link Long} value of the label. If it is not null.
     */
    public Long getGroupingLong(String label) {
        PrimitiveObject o = getPrimitiveObject(KEY, label, ObjectUtil.LONG, "Long");
        if (o == null) {
            return null;
        }
        return (Long) o.getObject();
    }

    /**
     * Get grouping {@link Double} value
     * @param label target label
     * @return {@link Double} value of the label. If it is not null.
     */
    public Double getGroupingDouble(String label) {
        PrimitiveObject o = getPrimitiveObject(KEY, label, ObjectUtil.DOUBLE, "Double");
        if (o == null) {
            return null;
        }
        return (Double) o.getObject();
    }

    /**
     * Get grouping {@link Float} value
     * @param label target label
     * @return {@link Float} value of the label. If it is not null.
     */
    public Float getGroupingFloat(String label) {
        PrimitiveObject o = getPrimitiveObject(KEY, label, ObjectUtil.FLOAT, "Float");
        if (o == null) {
            return null;
        }
        return (Float) o.getObject();
    }

    /**
     * Get grouping {@link Boolean} value
     * @param label target label
     * @return {@link Boolean} value of the label. If it is not null.
     */
    public Boolean getGroupingBoolean(String label) {
        PrimitiveObject o = getPrimitiveObject(KEY, label, ObjectUtil.BOOLEAN, "Boolean");
        if (o == null) {
            return null;
        }
        return (Boolean) o.getObject();
    }

    /**
     * Get grouping {@link Text} value
     * @param label target label
     * @return {@link Text} value of the label. If it is not null.
     */
    public Text getGroupingText(String label) {
        HadoopObject o = getHadoopObject(KEY, label, ObjectUtil.STRING, "String");
        if (o == null) {
            return null;
        }
        return (Text) o.getObject();
    }

    /**
     * Get grouping {@link ByteWritable} value
     * @param label target label
     * @return {@link ByteWritable} value of the label. If it is not null.
     */
    public ByteWritable getGroupingByteWritable(String label) {
        HadoopObject o = getHadoopObject(KEY, label, ObjectUtil.BYTE, "Byte");
        if (o == null) {
            return null;
        }
        return (ByteWritable) o.getObject();
    }

    /**
     * Get grouping {@link IntWritable} value
     * @param label target label
     * @return {@link IntWritable} value of the label. If it is not null.
     */
    public IntWritable getGroupingIntWritable(String label) {
        HadoopObject o = getHadoopObject(KEY, label, ObjectUtil.INTEGER, "Integer");
        if (o == null) {
            return null;
        }
        return (IntWritable) o.getObject();
    }

    /**
     * Get grouping {@link LongWritable} value
     * @param label target label
     * @return {@link LongWritable} value of the label. If it is not null.
     */
    public LongWritable getGroupingLongWritable(String label) {
        HadoopObject o = getHadoopObject(KEY, label, ObjectUtil.LONG, "Long");
        if (o == null) {
            return null;
        }
        return (LongWritable) o.getObject();
    }

    /**
     * Get grouping {@link DoubleWritable} value
     * @param label target label
     * @return {@link DoubleWritable} value of the label. If it is not null.
     */
    public DoubleWritable getGroupingDoubleWritable(String label) {
        HadoopObject o = getHadoopObject(KEY, label, ObjectUtil.DOUBLE, "Double");
        if (o == null) {
            return null;
        }
        return (DoubleWritable) o.getObject();
    }

    /**
     * Get grouping {@link FloatWritable} value
     * @param label target label
     * @return {@link FloatWritable} value of the label. If it is not null.
     */
    public FloatWritable getGroupingFloatWritable(String label) {
        HadoopObject o = getHadoopObject(KEY, label, ObjectUtil.FLOAT, "Float");
        if (o == null) {
            return null;
        }
        return (FloatWritable) o.getObject();
    }

    /**
     * Get grouping {@link BooleanWritable} value
     * @param label target label
     * @return {@link BooleanWritable} value of the label. If it is not null.
     */
    public BooleanWritable getGroupingBooleanWritable(String label) {
        HadoopObject o = getHadoopObject(KEY, label, ObjectUtil.BOOLEAN, "Boolean");
        if (o == null) {
            return null;
        }
        return (BooleanWritable) o.getObject();
    }

    /**
     * Get value {@link String} value
     * @param label target label
     * @return {@link String} value of the label. If it is not null.
     */
    public String getValueString(String label) {
        PrimitiveObject o = getPrimitiveObject(VALUE, label, ObjectUtil.STRING, "String");
        if (o == null) {
            return null;
        }
        return (String) o.getObject();
    }

    /**
     * Get value {@link Byte} value
     * @param label target label
     * @return {@link Byte} value of the label. If it is not null.
     */
    public Byte getValueByte(String label) {
        PrimitiveObject o = getPrimitiveObject(VALUE, label, ObjectUtil.BYTE, "Byte");
        if (o == null) {
            return null;
        }
        return (Byte) o.getObject();
    }

    /**
     * Get value {@link Integer} value
     * @param label target label
     * @return {@link Integer} value of the label. If it is not null.
     */
    public Integer getValueInteger(String label) {
        PrimitiveObject o = getPrimitiveObject(VALUE, label, ObjectUtil.INTEGER, "Integer");
        if (o == null) {
            return null;
        }
        return (Integer) o.getObject();
    }

    /**
     * Get value {@link Long} value
     * @param label target label
     * @return {@link Long} value of the label. If it is not null.
     */
    public Long getValueLong(String label) {
        PrimitiveObject o = getPrimitiveObject(VALUE, label, ObjectUtil.LONG, "Long");
        if (o == null) {
            return null;
        }
        return (Long) o.getObject();
    }

    /**
     * Get value {@link Double} value
     * @param label target label
     * @return {@link Double} value of the label. If it is not null.
     */
    public Double getValueDouble(String label) {
        PrimitiveObject o = getPrimitiveObject(VALUE, label, ObjectUtil.DOUBLE, "Double");
        if (o == null) {
            return null;
        }
        return (Double) o.getObject();
    }

    /**
     * Get value {@link Float} value
     * @param label target label
     * @return {@link Float} value of the label. If it is not null.
     */
    public Float getValueFloat(String label) {
        PrimitiveObject o = getPrimitiveObject(VALUE, label, ObjectUtil.FLOAT, "Float");
        if (o == null) {
            return null;
        }
        return (Float) o.getObject();
    }

    /**
     * Get value {@link Boolean} value
     * @param label target label
     * @return {@link Boolean} value of the label. If it is not null.
     */
    public Boolean getValueBoolean(String label) {
        PrimitiveObject o = getPrimitiveObject(VALUE, label, ObjectUtil.BOOLEAN, "Boolean");
        if (o == null) {
            return null;
        }
        return (Boolean) o.getObject();
    }

    /**
     * Get value {@link List} value
     * @param label target label
     * @return {@link List} value of the label. If it is not null.
     */
    public List<?> getValueList(String label) {
        PrimitiveObject o = getPrimitiveObject(VALUE, label, ObjectUtil.ARRAY, "Array");
        if (o == null) {
            return null;
        }

        return (List<?>) o.getObject();
    }

    /**
     * Get value {@link Map} value
     * @param label target label
     * @return {@link Map} value of the label. If it is not null.
     */
    public Map<?, ?> getValueMap(String label) {
        PrimitiveObject o = getPrimitiveObject(VALUE, label, ObjectUtil.MAP, "Map");
        if (o == null) {
            return null;
        }

        return (Map<?, ?>) o.getObject();
    }

    /**
     * Get value {@link Text} value
     * @param label target label
     * @return {@link Text} value of the label. If it is not null.
     */
    public Text getValueText(String label) {
        HadoopObject o = getHadoopObject(VALUE, label, ObjectUtil.STRING, "String");
        if (o == null) {
            return null;
        }
        return (Text) o.getObject();
    }

    /**
     * Get value {@link ByteWritable} value
     * @param label target label
     * @return {@link ByteWritable} value of the label. If it is not null.
     */
    public ByteWritable getValueByteWritable(String label) {
        HadoopObject o = getHadoopObject(VALUE, label, ObjectUtil.BYTE, "Byte");
        if (o == null) {
            return null;
        }
        return (ByteWritable) o.getObject();
    }

    /**
     * Get value {@link IntWritable} value
     * @param label target label
     * @return {@link IntWritable} value of the label. If it is not null.
     */
    public IntWritable getValueIntWritable(String label) {
        HadoopObject o = getHadoopObject(VALUE, label, ObjectUtil.INTEGER, "Integer");
        if (o == null) {
            return null;
        }
        return (IntWritable) o.getObject();
    }

    /**
     * Get value {@link LongWritable} value
     * @param label target label
     * @return {@link LongWritable} value of the label. If it is not null.
     */
    public LongWritable getValueLongWritable(String label) {
        HadoopObject o = getHadoopObject(VALUE, label, ObjectUtil.LONG, "Long");
        if (o == null) {
            return null;
        }
        return (LongWritable) o.getObject();
    }

    /**
     * Get value {@link DoubleWritable} value
     * @param label target label
     * @return {@link DoubleWritable} value of the label. If it is not null.
     */
    public DoubleWritable getValueDoubleWritable(String label) {
        HadoopObject o = getHadoopObject(VALUE, label, ObjectUtil.DOUBLE, "Double");
        if (o == null) {
            return null;
        }
        return (DoubleWritable) o.getObject();
    }

    /**
     * Get value {@link FloatWritable} value
     * @param label target label
     * @return {@link FloatWritable} value of the label. If it is not null.
     */
    public FloatWritable getValueFloatWritable(String label) {
        HadoopObject o = getHadoopObject(VALUE, label, ObjectUtil.FLOAT, "Float");
        if (o == null) {
            return null;
        }
        return (FloatWritable) o.getObject();
    }

    /**
     * Get value {@link BooleanWritable} value
     * @param label target label
     * @return {@link BooleanWritable} value of the label. If it is not null.
     */
    public BooleanWritable getValueBooleanWritable(String label) {
        HadoopObject o = getHadoopObject(VALUE, label, ObjectUtil.BOOLEAN, "Boolean");
        if (o == null) {
            return null;
        }
        return (BooleanWritable) o.getObject();
    }

    /**
     * Get value {@link ArrayWritable} value
     * @param label target label
     * @return {@link ArrayWritable} value of the label. If it is not null.
     */
    public ArrayWritable getValueArrayWritable(String label) {
        HadoopObject o = getHadoopObject(VALUE, label, ObjectUtil.ARRAY, "Array");
        if (o == null) {
            return null;
        }

        return (ArrayWritable) o.getObject();
    }

    /**
     * Get value {@link MapWritable} value
     * @param label target label
     * @return {@link MapWritable} value of the label. If it is not null.
     */
    public MapWritable getValueMapWritable(String label) {
        HadoopObject o = getHadoopObject(VALUE, label, ObjectUtil.MAP, "Map");
        if (o == null) {
            return null;
        }

        return (MapWritable) o.getObject();
    }

    /**
     * @param keyOrValue
     * @param label
     * @param type
     * @param objectName
     * @return {@link PrimitiveObject}
     */
    private PrimitiveObject getPrimitiveObject(int keyOrValue, String label, int type, String objectName) {
        PrimitiveObject o = null;
        if (keyOrValue == KEY) {
            o = key.getPrimitiveObject(label);
        } else {
            o = value.getPrimitiveObject(label);
        }

        if (o == null) {
            return null;
        }

        if (o.getType() != type) {
            throw new ClassCastException("object is not " + objectName + ": [" + o.getType() + "] = " + o.getObject());
        }

        return o;
    }

    /**
     * @param keyOrValue
     * @param label
     * @param type
     * @param objectName
     * @return {@link HadoopObject}
     */
    private HadoopObject getHadoopObject(int keyOrValue, String label, int type, String objectName) {
        HadoopObject o = null;
        if (keyOrValue == KEY) {
            o = key.getHadoopObject(label);
        } else {
            o = value.getHadoopObject(label);
        }

        if (o == null) {
            return null;
        }

        if (o.getType() != type) {
            throw new ClassCastException("object is not " + objectName + ": [" + o.getType() + "] = " + o.getObject());
        }

        return o;
    }
}
