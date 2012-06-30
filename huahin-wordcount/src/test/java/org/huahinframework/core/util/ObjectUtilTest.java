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

import java.util.ArrayList;
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
import org.huahinframework.core.util.HadoopObject;
import org.huahinframework.core.util.ObjectUtil;
import org.huahinframework.core.util.PrimitiveObject;
import org.junit.Test;

import junit.framework.TestCase;

/**
 *
 */
public class ObjectUtilTest extends TestCase {
    @Test
    public void testPrimitive2HadoopIONull() {
        HadoopObject o = ObjectUtil.primitive2Hadoop(null);
        assertEquals(ObjectUtil.NULL, o.getType());
        assertEquals(NullWritable.get(), o.getObject());
    }

    @Test
    public void testPrimitive2HadoopIOString() {
        String o = "string";
        HadoopObject ho = ObjectUtil.primitive2Hadoop(o);
        assertEquals(ObjectUtil.STRING, ho.getType());
        assertEquals(new Text(o), ho.getObject());
    }

    @Test
    public void testPrimitive2HadoopIOByte() {
        byte o = 123;
        HadoopObject ho = ObjectUtil.primitive2Hadoop(o);
        assertEquals(ObjectUtil.BYTE, ho.getType());
        assertEquals(new ByteWritable(o), ho.getObject());
    }

    @Test
    public void testPrimitive2HadoopIOInteger() {
        int o = 123;
        HadoopObject ho = ObjectUtil.primitive2Hadoop(o);
        assertEquals(ObjectUtil.INTEGER, ho.getType());
        assertEquals(new IntWritable(o), ho.getObject());
    }

    @Test
    public void testPrimitive2HadoopIOLong() {
        long o = 123;
        HadoopObject ho = ObjectUtil.primitive2Hadoop(o);
        assertEquals(ObjectUtil.LONG, ho.getType());
        assertEquals(new LongWritable(o), ho.getObject());
    }

    @Test
    public void testPrimitive2HadoopIODouble() {
        double o = 123;
        HadoopObject ho = ObjectUtil.primitive2Hadoop(o);
        assertEquals(ObjectUtil.DOUBLE, ho.getType());
        assertEquals(new DoubleWritable(o), ho.getObject());
    }

    @Test
    public void testPrimitive2HadoopIOFloat() {
        float o = 123;
        HadoopObject ho = ObjectUtil.primitive2Hadoop(o);
        assertEquals(ObjectUtil.FLOAT, ho.getType());
        assertEquals(new FloatWritable(o), ho.getObject());
    }

    @Test
    public void testPrimitive2HadoopIOBoolean() {
        boolean o = true;
        HadoopObject ho = ObjectUtil.primitive2Hadoop(o);
        assertEquals(ObjectUtil.BOOLEAN, ho.getType());
        assertEquals(new BooleanWritable(o), ho.getObject());
    }

    @Test
    public void testPrimitive2HadoopIOArray() {
        int[] o = new int[5];
        IntWritable[] iw = new IntWritable[5];
        for (int i = 0; i < o.length; i++) {
            o[i] = i;
            iw[i] = new IntWritable(i);
        }
        HadoopObject ho = ObjectUtil.primitive2Hadoop(o);
        assertEquals(ObjectUtil.ARRAY, ho.getType());
        assertEquals(ArrayWritable.class, ho.getObject().getClass());

        ArrayWritable aw = (ArrayWritable) ho.getObject();
        Writable[] w = aw.get();

        if (w.length != iw.length) {
            fail("array not equals length");
        }

        for (int i = 0; i < w.length; i++) {
            assertEquals(w[i], iw[i]);
        }
    }

    @Test
    public void testPrimitive2HadoopIOCollection() {
        List<Integer> o = new ArrayList<Integer>();
        IntWritable[] iw = new IntWritable[2];
        o.add(0);
        iw[0] = new IntWritable(0);

        o.add(1);
        iw[1] = new IntWritable(1);

        HadoopObject ho = ObjectUtil.primitive2Hadoop(o);
        assertEquals(ObjectUtil.ARRAY, ho.getType());
        assertEquals(ArrayWritable.class, ho.getObject().getClass());

        ArrayWritable aw = (ArrayWritable) ho.getObject();
        Writable[] w = aw.get();
        if (w.length != iw.length) {
            fail("array not equals length");
        }

        for (int i = 0; i < w.length; i++) {
            assertEquals(w[i], iw[i]);
        }
    }

    @Test
    public void testPrimitive2HadoopIOMap() {
        Map<String, Integer> o = new HashMap<String, Integer>();
        MapWritable m = new MapWritable();

        o.put("0", 0);
        m.put(new Text("0"), new IntWritable(0));

        o.put("1", 1);
        m.put(new Text("1"), new IntWritable(1));

        HadoopObject ho = ObjectUtil.primitive2Hadoop(o);
        assertEquals(ObjectUtil.MAP, ho.getType());
        assertEquals(MapWritable.class, ho.getObject().getClass());

        MapWritable mw = (MapWritable) ho.getObject();
        if (mw.size() != m.size()) {
            fail("map not equals size: " + mw.size() + " != " + m.size());
        }

        for (Entry<Writable, Writable> entry : m.entrySet()) {
            if (mw.get(entry.getKey()) == null) {
                fail("map key not found");
            }

            assertEquals(mw.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void testHadoopIO2PrimitiveNull() {
        PrimitiveObject o = ObjectUtil.hadoop2Primitive(NullWritable.get());
        assertEquals(ObjectUtil.NULL, o.getType());
        assertEquals(null, o.getObject());
    }

    @Test
    public void testHadoopIO2PrimitiveString() {
        String o = "string";
        PrimitiveObject no = ObjectUtil.hadoop2Primitive(new Text(o));
        assertEquals(ObjectUtil.STRING, no.getType());
        assertEquals(o, no.getObject());
    }

    @Test
    public void testHadoopIO2PrimitiveByte() {
        byte o = 123;
        PrimitiveObject no = ObjectUtil.hadoop2Primitive(new ByteWritable(o));
        assertEquals(ObjectUtil.BYTE, no.getType());
        assertEquals(o, no.getObject());
    }

    @Test
    public void testHadoopIO2PrimitiveInteger() {
        int o = 123;
        PrimitiveObject no = ObjectUtil.hadoop2Primitive(new IntWritable(o));
        assertEquals(ObjectUtil.INTEGER, no.getType());
        assertEquals(o, no.getObject());
    }

    @Test
    public void testHadoopIO2PrimitiveLong() {
        long o = 123;
        PrimitiveObject no = ObjectUtil.hadoop2Primitive(new LongWritable(o));
        assertEquals(ObjectUtil.LONG, no.getType());
        assertEquals(o, no.getObject());
    }

    @Test
    public void testHadoopIO2PrimitiveDouble() {
        double o = 123;
        PrimitiveObject no = ObjectUtil.hadoop2Primitive(new DoubleWritable(o));
        assertEquals(ObjectUtil.DOUBLE, no.getType());
        assertEquals(o, no.getObject());
    }

    @Test
    public void testHadoopIO2PrimitiveFloat() {
        float o = 123;
        PrimitiveObject no = ObjectUtil.hadoop2Primitive(new FloatWritable(o));
        assertEquals(ObjectUtil.FLOAT, no.getType());
        assertEquals(o, no.getObject());
    }

    @Test
    public void testHadoopIO2PrimitiveBoolean() {
        boolean o = true;
        PrimitiveObject no = ObjectUtil.hadoop2Primitive(new BooleanWritable(o));
        assertEquals(ObjectUtil.BOOLEAN, no.getType());
        assertEquals(o, no.getObject());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testHadoopIO2PrimitiveCollection() {
        List<Integer> o = new ArrayList<Integer>();
        IntWritable[] iw = new IntWritable[2];
        o.add(0);
        iw[0] = new IntWritable(0);

        o.add(1);
        iw[1] = new IntWritable(1);
        ArrayWritable aw = new ArrayWritable(IntWritable.class, iw);

        PrimitiveObject no = ObjectUtil.hadoop2Primitive(aw);
        assertEquals(ObjectUtil.ARRAY, no.getType());
        assertEquals(ObjectUtil.INTEGER, no.getArrayType());
        if (!(no.getObject() instanceof List<?>)) {
            fail("object not collection");
        }

        List<Integer> l = (List<Integer>) no.getObject();
        if (l.size() != o.size()) {
            fail("array not equals length");
        }

        for (int i = 0; i < l.size(); i++) {
            assertEquals(l.get(i), o.get(i));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testHadoopIO2PrimitiveMap() {
        Map<String, Integer> o = new HashMap<String, Integer>();
        MapWritable mw = new MapWritable();

        o.put("0", 0);
        mw.put(new Text("0"), new IntWritable(0));

        o.put("1", 1);
        mw.put(new Text("1"), new IntWritable(1));

        PrimitiveObject no = ObjectUtil.hadoop2Primitive(mw);
        assertEquals(ObjectUtil.MAP, no.getType());
        assertEquals(ObjectUtil.STRING, no.getMapKeyType());
        assertEquals(ObjectUtil.INTEGER, no.getMapValueType());
        if (!(no.getObject() instanceof Map<?, ?>)) {
            fail("object not map");
        }

        Map<String, Integer> m = (Map<String, Integer>) no.getObject();
        if (mw.size() != o.size()) {
            fail("map not equals size: " + mw.size() + " != " + o.size());
        }

        for (Entry<String, Integer> entry : o.entrySet()) {
            if (m.get(entry.getKey()) == null) {
                fail("map key not found");
            }

            assertEquals(m.get(entry.getKey()), entry.getValue());
        }
    }
}
