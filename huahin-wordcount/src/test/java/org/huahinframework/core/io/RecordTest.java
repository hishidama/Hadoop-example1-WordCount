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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
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
import org.huahinframework.core.io.Record;
import org.junit.Test;

/**
 *
 */
public class RecordTest {
    @Test
    public void testGroupingString() {
        Record record = new Record();
        String o = "String";
        record.addGrouping("Object", o);
        assertEquals(record.getGroupingString("Object"), o);
        assertEquals(record.getGroupingString("Object2"), null);

        try {
            record.getGroupingInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testGroupingByte() {
        Record record = new Record();
        Byte o = 10;
        record.addGrouping("Object", o);
        assertEquals(record.getGroupingByte("Object"), o);
        assertEquals(record.getGroupingString("Object2"), null);

        try {
            record.getGroupingInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testGroupingInteger() {
        Record record = new Record();
        Integer o = 10;
        record.addGrouping("Object", o);
        assertEquals(record.getGroupingInteger("Object"), o);
        assertEquals(record.getGroupingInteger("Object2"), null);

        try {
            record.getGroupingLong("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testGroupingLong() {
        Record record = new Record();
        Long o = 10L;
        record.addGrouping("Object", o);
        assertEquals(record.getGroupingLong("Object"), o);
        assertEquals(record.getGroupingLong("Object2"), null);

        try {
            record.getGroupingInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testGroupingDouble() {
        Record record = new Record();
        Double o = 10.0;
        record.addGrouping("Object", o);
        assertEquals(record.getGroupingDouble("Object"), o);
        assertEquals(record.getGroupingDouble("Object2"), null);

        try {
            record.getGroupingInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testGroupingFloat() {
        Record record = new Record();
        Float o = 10.0F;
        record.addGrouping("Object", o);
        assertEquals(record.getGroupingFloat("Object"), o);
        assertEquals(record.getGroupingFloat("Object2"), null);

        try {
            record.getGroupingInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testGroupingBoolean() {
        Record record = new Record();
        Boolean o = true;
        record.addGrouping("Object", o);
        assertEquals(record.getGroupingBoolean("Object"), o);
        assertEquals(record.getGroupingBoolean("Object2"), null);

        try {
            record.getGroupingInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testGroupingText() {
        Record record = new Record();
        Text o = new Text("String");
        record.addGrouping("Object", o);
        assertEquals(record.getGroupingText("Object"), o);
        assertEquals(record.getGroupingText("Object2"), null);

        try {
            record.getGroupingInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testGroupingByteWritable() {
        Record record = new Record();
        ByteWritable o = new ByteWritable((byte) 10);
        record.addGrouping("Object", o);
        assertEquals(record.getGroupingByteWritable("Object"), o);
        assertEquals(record.getGroupingByteWritable("Object2"), null);

        try {
            record.getGroupingInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testGroupingIntWritable() {
        Record record = new Record();
        IntWritable o = new IntWritable(10);
        record.addGrouping("Object", o);
        assertEquals(record.getGroupingIntWritable("Object"), o);
        assertEquals(record.getGroupingIntWritable("Object2"), null);

        try {
            record.getGroupingLongWritable("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testGroupingLongWritable() {
        Record record = new Record();
        LongWritable o = new LongWritable(10L);
        record.addGrouping("Object", o);
        assertEquals(record.getGroupingLongWritable("Object"), o);
        assertEquals(record.getGroupingLongWritable("Object2"), null);

        try {
            record.getGroupingInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testGroupingDoubleWritable() {
        Record record = new Record();
        DoubleWritable o = new DoubleWritable(10.0);
        record.addGrouping("Object", o);
        assertEquals(record.getGroupingDoubleWritable("Object"), o);
        assertEquals(record.getGroupingDoubleWritable("Object2"), null);

        try {
            record.getGroupingInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testGroupingFloatWritable() {
        Record record = new Record();
        FloatWritable o = new FloatWritable(10.0F);
        record.addGrouping("Object", o);
        assertEquals(record.getGroupingFloatWritable("Object"), o);
        assertEquals(record.getGroupingFloatWritable("Object2"), null);

        try {
            record.getGroupingInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testGroupingBooleanWritable() {
        Record record = new Record();
        BooleanWritable o = new BooleanWritable(true);
        record.addGrouping("Object", o);
        assertEquals(record.getGroupingBooleanWritable("Object"), o);
        assertEquals(record.getGroupingBooleanWritable("Object2"), null);

        try {
            record.getGroupingInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testSetSortPrimitive() {
        Record record = new Record();
        String o = "String";
        record.addSort(o, Record.SORT_LOWER, 1);
        assertEquals(record.getGroupingString("Object"), null);
    }

    @Test
    public void testSetSortHadoop() {
        Record record = new Record();
        Text o = new Text("String");
        record.addSort(o, Record.SORT_LOWER, 1);
        assertEquals(record.getGroupingString("Object"), null);
    }

    @Test
    public void testValueString() {
        Record record = new Record();
        String o = "String";
        record.addValue("Object", o);
        assertEquals(record.getValueString("Object"), o);
        assertEquals(record.getValueString("Object2"), null);

        try {
            record.getValueInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueByte() {
        Record record = new Record();
        Byte o = 10;
        record.addValue("Object", o);
        assertEquals(record.getValueByte("Object"), o);
        assertEquals(record.getValueByte("Object2"), null);

        try {
            record.getValueInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueInteger() {
        Record record = new Record();
        Integer o = 10;
        record.addValue("Object", o);
        assertEquals(record.getValueInteger("Object"), o);
        assertEquals(record.getValueInteger("Object2"), null);

        try {
            record.getValueLong("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueLong() {
        Record record = new Record();
        Long o = 10L;
        record.addValue("Object", o);
        assertEquals(record.getValueLong("Object"), o);
        assertEquals(record.getValueLong("Object2"), null);

        try {
            record.getValueInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValuDouble() {
        Record record = new Record();
        Double o = 10.0;
        record.addValue("Object", o);
        assertEquals(record.getValueDouble("Object"), o);
        assertEquals(record.getValueDouble("Object2"), null);

        try {
            record.getValueInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueFloat() {
        Record record = new Record();
        Float o = 10.0F;
        record.addValue("Object", o);
        assertEquals(record.getValueFloat("Object"), o);
        assertEquals(record.getValueFloat("Object2"), null);

        try {
            record.getValueInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueBoolean() {
        Record record = new Record();
        Boolean o = true;
        record.addValue("Object", o);
        assertEquals(record.getValueBoolean("Object"), o);
        assertEquals(record.getValueBoolean("Object2"), null);

        try {
            record.getValueInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueStringList() {
        Record record = new Record();
        List<String> o = new ArrayList<String>();
        o.add("String1");
        o.add("String2");
        o.add("String3");
        record.addValue("Object", o);
        assertEquals(record.getValueList("Object"), o);
        assertEquals(record.getValueList("Object").size(), 3);

        assertEquals(record.getValueList("Object").get(0), "String1");
        assertEquals(record.getValueList("Object").get(1), "String2");
        assertEquals(record.getValueList("Object").get(2), "String3");

        assertEquals(record.getValueList("Object2"), null);

        try {
            record.getValueInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueIntegerList() {
        Record record = new Record();
        List<Integer> o = new ArrayList<Integer>();
        o.add(0);
        o.add(1);
        o.add(2);
        record.addValue("Object", o);
        assertEquals(record.getValueList("Object"), o);
        assertEquals(record.getValueList("Object").size(), 3);

        assertEquals(record.getValueList("Object").get(0), new Integer(0));
        assertEquals(record.getValueList("Object").get(1), new Integer(1));
        assertEquals(record.getValueList("Object").get(2), new Integer(2));

        assertEquals(record.getValueList("Object2"), null);

        try {
            record.getValueInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueMapString() {
        Record record = new Record();
        Map<String, String> o = new HashMap<String, String>();
        o.put("String1", "String1");
        o.put("String2", "String2");
        o.put("String3", "String3");
        record.addValue("Object", o);
        assertEquals(record.getValueMap("Object"), o);
        assertEquals(record.getValueMap("Object").size(), 3);

        assertEquals(record.getValueMap("Object").get("String1"), "String1");
        assertEquals(record.getValueMap("Object").get("String2"), "String2");
        assertEquals(record.getValueMap("Object").get("String3"), "String3");

        assertEquals(record.getValueList("Object2"), null);

        try {
            record.getValueInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueMapStringInteger() {
        Record record = new Record();
        Map<String, Integer> o = new HashMap<String, Integer>();
        o.put("String1", 1);
        o.put("String2", 2);
        o.put("String3", 3);
        record.addValue("Object", o);
        assertEquals(record.getValueMap("Object"), o);
        assertEquals(record.getValueMap("Object").size(), 3);

        assertEquals(record.getValueMap("Object").get("String1"), 1);
        assertEquals(record.getValueMap("Object").get("String2"), 2);
        assertEquals(record.getValueMap("Object").get("String3"), 3);

        assertEquals(record.getValueList("Object2"), null);

        try {
            record.getValueInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueText() {
        Record record = new Record();
        Text o = new Text("String");
        record.addValue("Object", o);
        assertEquals(record.getValueText("Object"), o);
        assertEquals(record.getValueText("Object2"), null);

        try {
            record.getValueIntWritable("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueByteWritable() {
        Record record = new Record();
        ByteWritable o = new ByteWritable((byte) 10);
        record.addValue("Object", o);
        assertEquals(record.getValueByteWritable("Object"), o);
        assertEquals(record.getValueByteWritable("Object2"), null);

        try {
            record.getValueIntWritable("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueIntWritable() {
        Record record = new Record();
        IntWritable o = new IntWritable(10);
        record.addValue("Object", o);
        assertEquals(record.getValueIntWritable("Object"), o);
        assertEquals(record.getValueIntWritable("Object2"), null);

        try {
            record.getValueLongWritable("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueLongWritable() {
        Record record = new Record();
        LongWritable o = new LongWritable(10L);
        record.addValue("Object", o);
        assertEquals(record.getValueLongWritable("Object"), o);
        assertEquals(record.getValueLongWritable("Object2"), null);

        try {
            record.getValueIntWritable("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueDoubleWritable() {
        Record record = new Record();
        DoubleWritable o = new DoubleWritable(10.0);
        record.addValue("Object", o);
        assertEquals(record.getValueDoubleWritable("Object"), o);
        assertEquals(record.getValueDoubleWritable("Object2"), null);

        try {
            record.getValueIntWritable("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueFloatWritable() {
        Record record = new Record();
        FloatWritable o = new FloatWritable(10.0F);
        record.addValue("Object", o);
        assertEquals(record.getValueFloatWritable("Object"), o);
        assertEquals(record.getValueFloatWritable("Object2"), null);

        try {
            record.getValueIntWritable("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueBooleanWritable() {
        Record record = new Record();
        BooleanWritable o = new BooleanWritable(true);
        record.addValue("Object", o);
        assertEquals(record.getValueBooleanWritable("Object"), o);
        assertEquals(record.getValueBooleanWritable("Object2"), null);

        try {
            record.getValueIntWritable("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueTextArray() {
        Record record = new Record();
        ArrayWritable o = new ArrayWritable(Text.class);
        Writable[] w = {new Text("String1"), new Text("String2"), new Text("String3")};
        o.set(w);
        record.addValue("Object", o);
        assertEquals(record.getValueArrayWritable("Object"), o);
        assertEquals(record.getValueArrayWritable("Object").get().length, 3);

        assertEquals(record.getValueArrayWritable("Object").get()[0], new Text("String1"));
        assertEquals(record.getValueArrayWritable("Object").get()[1], new Text("String2"));
        assertEquals(record.getValueArrayWritable("Object").get()[2], new Text("String3"));

        assertEquals(record.getValueArrayWritable("Object2"), null);
        try {
            record.getValueInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueIntWritableArray() {
        Record record = new Record();
        ArrayWritable o = new ArrayWritable(Text.class);
        Writable[] w = {new IntWritable(1), new IntWritable(2), new IntWritable(3)};
        o.set(w);
        record.addValue("Object", o);
        assertEquals(record.getValueArrayWritable("Object"), o);
        assertEquals(record.getValueArrayWritable("Object").get().length, 3);

        assertEquals(record.getValueArrayWritable("Object").get()[0], new IntWritable(1));
        assertEquals(record.getValueArrayWritable("Object").get()[1], new IntWritable(2));
        assertEquals(record.getValueArrayWritable("Object").get()[2], new IntWritable(3));

        assertEquals(record.getValueArrayWritable("Object2"), null);
        try {
            record.getValueInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueTextMapWritable() {
        Record record = new Record();
        MapWritable o = new MapWritable();
        o.put(new Text("String1"), new Text("String1"));
        o.put(new Text("String2"), new Text("String2"));
        o.put(new Text("String3"), new Text("String3"));
        record.addValue("Object", o);
        assertEquals(record.getValueMapWritable("Object"), o);
        assertEquals(record.getValueMapWritable("Object").size(), 3);

        assertEquals(record.getValueMapWritable("Object").get(new Text("String1")), new Text("String1"));
        assertEquals(record.getValueMapWritable("Object").get(new Text("String2")), new Text("String2"));
        assertEquals(record.getValueMapWritable("Object").get(new Text("String3")), new Text("String3"));

        assertEquals(record.getValueList("Object2"), null);

        try {
            record.getValueInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

    @Test
    public void testValueTextIntWritableMapWritable() {
        Record record = new Record();
        MapWritable o = new MapWritable();
        o.put(new Text("String1"), new IntWritable(1));
        o.put(new Text("String2"), new IntWritable(2));
        o.put(new Text("String3"), new IntWritable(3));
        record.addValue("Object", o);
        assertEquals(record.getValueMapWritable("Object"), o);
        assertEquals(record.getValueMapWritable("Object").size(), 3);

        assertEquals(record.getValueMapWritable("Object").get(new Text("String1")), new IntWritable(1));
        assertEquals(record.getValueMapWritable("Object").get(new Text("String2")), new IntWritable(2));
        assertEquals(record.getValueMapWritable("Object").get(new Text("String3")), new IntWritable(3));

        assertEquals(record.getValueList("Object2"), null);

        try {
            record.getValueInteger("Object");
            fail("fail ClassCastException");
        } catch (Exception e) {
            assertTrue(e instanceof ClassCastException);
        }
    }
}
