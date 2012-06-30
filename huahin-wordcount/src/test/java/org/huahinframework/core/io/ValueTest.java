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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.huahinframework.core.io.Value;
import org.junit.Test;

/**
 *
 */
public class ValueTest {
    private class ValueTestMapper extends Mapper<LongWritable, Text, LongWritable, Value> {
        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException ,InterruptedException {
            String[] inputs = value.toString().split("\t");
            for (String s : inputs) {
                Value emitValue = new Value();
                emitValue.addPrimitiveValue("word", s);
                context.write(key, emitValue);
            }
        }
    }

    private class ValueNullTestMapper extends Mapper<LongWritable, Text, LongWritable, Value> {
        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException ,InterruptedException {
            Value emitValue = new Value();
            emitValue.addPrimitiveValue("", null);
            context.write(key, emitValue);
        }
    }

    private Mapper<LongWritable, Text, LongWritable, Value> mapper;
    private MapDriver<LongWritable, Text, LongWritable, Value> driver;

    @Test
    public void testKeyEquals() {
        Map<String, Integer> m = new HashMap<String, Integer>();
        m.put("key1", 1);
        m.put("key2", 1);

        Value value1 = new Value();
        value1.addPrimitiveValue("word", "foo");
        value1.addPrimitiveValue("count", 1);
        value1.addPrimitiveValue("value", "bar");
        value1.addPrimitiveValue("array", Arrays.asList("a", "b", "c"));
        value1.addPrimitiveValue("map", m);

        Value value2 = new Value();
        value2.addPrimitiveValue("word", "foo");
        value2.addPrimitiveValue("count", 1);
        value2.addPrimitiveValue("value", "bar");
        value2.addPrimitiveValue("array", Arrays.asList("a", "b", "c"));
        value2.addPrimitiveValue("map", m);

        Value value3 = new Value();
        value3.addPrimitiveValue("word", "foo");
        value3.addPrimitiveValue("count", 3);
        value3.addPrimitiveValue("value", "hoge");

        assertEquals(value1.hashCode(), value2.hashCode());
        assertEquals(value1.toString(), value2.toString());
        assertTrue(value1.equals(value2));

        assertFalse(value1.equals(value3));
    }

    @Test
    public void testGetValue() {
        Value value = new Value();
        value.addPrimitiveValue("word", "foo");
        value.addPrimitiveValue("count", 1);
        value.addPrimitiveValue("value", "bar");

        assertEquals(value.getPrimitiveValue("word"), "foo");
        assertEquals(value.getPrimitiveValue("count"), 1);
        assertEquals(value.getPrimitiveValue("value"), "bar");
        assertEquals(value.getPrimitiveValue("foo"), null);
    }

    @Test
    public void testValueMapper() {
        Value value1 = new Value();
        value1.addPrimitiveValue("word", "111");

        Value value2 = new Value();
        value2.addPrimitiveValue("word", "222");

        Value value3 = new Value();
        value3.addPrimitiveValue("word", "333");

        Value value4 = new Value();
        value4.addPrimitiveValue("word", "444");

        LongWritable key = new LongWritable(1L);
        mapper = new ValueTestMapper();
        driver = new MapDriver<LongWritable, Text, LongWritable, Value>(mapper);
        driver.withInput(new LongWritable(1L), new Text("111\t222\t333\t444"));
        driver.withOutput(key, value1);
        driver.withOutput(key, value2);
        driver.withOutput(key, value3);
        driver.withOutput(key, value4);
        driver.runTest();
    }

    @Test
    public void testNull() {
        Value value = new Value();
        value.addPrimitiveValue("", null);
        assertEquals(value.toString(), "(null)");
    }

    @Test
    public void testNullValueMapper() {
        Value value = new Value();
        value.addPrimitiveValue("", null);

        LongWritable key = new LongWritable(1L);
        mapper = new ValueNullTestMapper();
        driver = new MapDriver<LongWritable, Text, LongWritable, Value>(mapper);
        driver.withInput(new LongWritable(1L), new Text("null"));
        driver.withOutput(key, value);
        driver.runTest();
    }
}
