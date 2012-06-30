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
package org.huahinframework.core.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * <code>ObjectUtil</code> is a utility to manipulate the of Java primitive and Hadoop {@link Writable}.
 */
public class ObjectUtil {
    /**
     * Object type is null
     */
    public static final int NULL = 0;

    /**
     * Object type is {@link String}
     */
    public static final int STRING = 11;

    /**
     * Object type is {@link Byte}
     */
    public static final int BYTE = 21;

    /**
     * Object type is {@link Integer}
     */
    public static final int INTEGER = 22;

    /**
     * Object type is {@link Long}
     */
    public static final int LONG = 23;

    /**
     * Object type is {@link Double}
     */
    public static final int DOUBLE = 24;

    /**
     * Object type is {@link Float}
     */
    public static final int FLOAT = 25;

    /**
     * Object type is {@link Boolean}
     */
    public static final int BOOLEAN = 26;

    /**
     * Object type is {@link Collection}
     */
    public static final int ARRAY = 31;

    /**
     * Object type is {@link Map}
     */
    public static final int MAP = 32;

    public static final Map<Integer, Class<? extends Writable>> hadoopObjectMap = new HashMap<Integer, Class<? extends Writable>>();
    public static final Map<Integer, Class<?>> primitiveObjectMap = new HashMap<Integer, Class<?>>();
    static {
        hadoopObjectMap.put(NULL, NullWritable.class);
        hadoopObjectMap.put(STRING, Text.class);
        hadoopObjectMap.put(BYTE, ByteWritable.class);
        hadoopObjectMap.put(INTEGER, IntWritable.class);
        hadoopObjectMap.put(LONG, LongWritable.class);
        hadoopObjectMap.put(DOUBLE, DoubleWritable.class);
        hadoopObjectMap.put(FLOAT, FloatWritable.class);
        hadoopObjectMap.put(BOOLEAN, BooleanWritable.class);

        primitiveObjectMap.put(STRING, String.class);
        primitiveObjectMap.put(BYTE, Byte.class);
        primitiveObjectMap.put(INTEGER, Integer.class);
        primitiveObjectMap.put(LONG, Long.class);
        primitiveObjectMap.put(DOUBLE, Double.class);
        primitiveObjectMap.put(FLOAT, Float.class);
        primitiveObjectMap.put(BOOLEAN, Boolean.class);
    }

    /**
     * Convert the HadoopObject from Java primitive.
     * @param object Java primitive object
     * @return HadoopObject
     */
    public static HadoopObject primitive2Hadoop(Object object) {
        if (object == null) {
            return new HadoopObject(NULL, NullWritable.get());
        }

        if (object instanceof Byte) {
            return new HadoopObject(BYTE, new ByteWritable((Byte) object));
        } else if (object instanceof Integer) {
            return new HadoopObject(INTEGER, new IntWritable((Integer) object));
        } else if (object instanceof Long) {
            return new HadoopObject(LONG, new LongWritable((Long) object));
        } else if (object instanceof Double) {
            return new HadoopObject(DOUBLE, new DoubleWritable((Double) object));
        } else if (object instanceof Float) {
            return new HadoopObject(FLOAT, new FloatWritable((Float) object));
        } else if (object instanceof Boolean) {
            return new HadoopObject(BOOLEAN, new BooleanWritable((Boolean) object));
        } else if (object instanceof String) {
            return new HadoopObject(STRING, new Text((String) object));
        } else if (object.getClass().isArray()) {
            return arrayPrimitive2Hadoop(object);
        } else if (object instanceof Collection<?>) {
            Collection<?> collection = (Collection<?>) object;
            return arrayPrimitive2Hadoop(collection.toArray());
        } else if (object instanceof Map<?, ?>) {
            Map<?, ?> map = (Map<?, ?>) object;
            if (map.size() == 0) {
                throw new ClassCastException("object not found");
            }

            MapWritable mapWritable = new MapWritable();
            for (Entry<?, ?> entry : map.entrySet()) {
                mapWritable.put(primitive2Hadoop(entry.getKey()).getObject(),
                                primitive2Hadoop(entry.getValue()).getObject());
            }

            return new HadoopObject(MAP, mapWritable);
        }

        throw new ClassCastException("cast object not found");
    }

    /**
     * @param objects
     * @return HadoopObject
     */
    private static HadoopObject arrayPrimitive2Hadoop(Object object) {
        if (!object.getClass().isArray()) {
            throw new ClassCastException();
        }

        int length = Array.getLength(object);
        if (length == 0) {
            throw new ClassCastException("object not found");
        }

        Writable[] writables = new Writable[length];
        int type = NULL;
        for (int i = 0; i < length; i++) {
            HadoopObject hadoopObject = primitive2Hadoop(Array.get(object, i));
            type = hadoopObject.getType();
            writables[i] = hadoopObject.getObject();
        }

        return new HadoopObject(ARRAY, new ArrayWritable(hadoopObjectMap.get(type), writables));
    }

    /**
     * Convert the PrimitiveObject from Hadoop {@link Writable}.
     * @param object
     * @return PrimitiveObject
     */
    public static PrimitiveObject hadoop2Primitive(Writable object) {
        if (object instanceof NullWritable) {
            return new PrimitiveObject(NULL, null);
        }

        if (object instanceof ByteWritable) {
            return new PrimitiveObject(BYTE, ((ByteWritable) object).get());
        } else if (object instanceof IntWritable) {
            return new PrimitiveObject(INTEGER, ((IntWritable) object).get());
        } else if (object instanceof LongWritable) {
            return new PrimitiveObject(LONG, ((LongWritable) object).get());
        } else if (object instanceof DoubleWritable) {
            return new PrimitiveObject(DOUBLE, ((DoubleWritable) object).get());
        } else if (object instanceof FloatWritable) {
            return new PrimitiveObject(FLOAT, ((FloatWritable) object).get());
        } else if (object instanceof BooleanWritable) {
            return new PrimitiveObject(BOOLEAN, ((BooleanWritable) object).get());
        } else if (object instanceof Text) {
            return new PrimitiveObject(STRING, ((Text) object).toString());
        } else if (object instanceof ArrayWritable) {
            ArrayWritable aw = (ArrayWritable) object;
            if (aw.get().length == 0) {
                return new PrimitiveObject(ARRAY, true, STRING, new ArrayList<String>());
            }

            int type = NULL;
            List<Object> l = new ArrayList<Object>();
            for (Writable w : aw.get()){
                PrimitiveObject no = hadoop2Primitive(w);
                type = no.getType();
                l.add(no.getObject());
            }

            return new PrimitiveObject(ARRAY, true, type, l);
        } else if (object instanceof MapWritable) {
            MapWritable mw = (MapWritable) object;
            if (mw.size() == 0) {
                return new PrimitiveObject(ARRAY, true, STRING, STRING, new HashMap<String, String>());
            }

            int keyType = NULL;
            int valueType = NULL;
            Map<Object, Object> m = new HashMap<Object, Object>();
            for (Entry<Writable, Writable> entry : mw.entrySet()){
                PrimitiveObject keyNo = hadoop2Primitive(entry.getKey());
                PrimitiveObject valueNo = hadoop2Primitive(entry.getValue());
                keyType = keyNo.getType();
                valueType = valueNo.getType();
                m.put(keyNo.getObject(), valueNo.getObject());
            }

            return new PrimitiveObject(MAP, true, keyType, valueType, m);
        }

        throw new ClassCastException("cast object not found");
    }

    /**
     * Compare the Writable.
     * @param one the original object
     * @param other the object to be compared.
     * @return if 0, are equal.
     */
    public static int typeCompareTo(Writable one, Writable other) {
        PrimitiveObject noOne = hadoop2Primitive(one);
        PrimitiveObject noOther = hadoop2Primitive(other);
        if (noOne.getType() != noOther.getType()) {
            return -1;
        }

        return 0;
    }
}
