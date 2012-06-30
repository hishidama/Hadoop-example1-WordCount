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
package org.huahinframework.core;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static org.junit.Assert.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import org.huahinframework.core.io.Key;
import org.huahinframework.core.io.Value;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class KeyValueMapReduceTest {
    private class KeyValueMapTestMapper extends Mapper<LongWritable, Text, Key, Value> {
        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException ,InterruptedException {
            Key emitKey = new Key();
            Value emitValue = new Value();

            String[] inputs = value.toString().split("\t");

            int valueCount = 0;
            StringBuilder sb = new StringBuilder();
            for (String s : inputs) {
                sb.append(s);
                emitValue.addPrimitiveValue("value" + ++valueCount, s);
            }
            emitKey.addPrimitiveValue("key", sb.toString());
            emitValue.addPrimitiveValue("size", valueCount);

            context.write(emitKey, emitValue);
        }
    }

    private class KeyValueMapTestReducer extends Reducer<Key, Value, Key, Value> {
        public void reduce(Key key, Iterable<Value> values, Context context)
                throws java.io.IOException ,InterruptedException {
            Map<String, Integer> m = new TreeMap<String, Integer>();
            for (Value v : values) {
                int size = (Integer) v.getPrimitiveValue("size");
                for (int i = 1; i <= size; i++) {
                    String s = (String) v.getPrimitiveValue("value" + i);
                    if (m.get(s) == null) {
                        m.put(s, 1);
                    } else {
                        m.put(s, m.get(s) + 1);
                    }
                }
            }

            for (Entry<String, Integer> entry : m.entrySet()) {
                Value emitValue = new Value();
                emitValue.addPrimitiveValue("value", entry.getKey() + "\t" + entry.getValue());
                context.write(key, emitValue);
            }
        }
    }

    private MapReduceDriver<LongWritable, Text, Key, Value, Key, Value> driver;

    @Before
    public void setUp() throws Exception {
        driver =
                MapReduceDriver.newMapReduceDriver(new KeyValueMapTestMapper(),
                                                   new KeyValueMapTestReducer());
    }

    @Test
    public void test() throws IOException {
        driver.addInput(new LongWritable(1L), new Text("a\tb\tc\td"));
        driver.addInput(new LongWritable(1L), new Text("a\tb\tc\td"));
        driver.addInput(new LongWritable(1L), new Text("a\tb\tc\td"));
        driver.addInput(new LongWritable(1L), new Text("d\tc\tb\ta"));

        List<Pair<Key, Value>> actual = driver.run();
        for (Pair<Key, Value> pair : actual) {
            System.out.println(pair);
        }
        assertEquals(8, actual.size());

        Pair<Key, Value> result;
        Key key;
        Value value;

        Key resultKey = new Key();
        Value resultValue = new Value();


        // ========= key0 =========
        resultKey.clear();
        resultKey.addPrimitiveValue("key", "abcd");

        // ---------- 0 ----------
        result = actual.get(0);
        key = result.getFirst();
        value = result.getSecond();

        resultValue.clear();
        resultValue.addPrimitiveValue("value", "a\t3");
        assertEquals(key, resultKey);
        assertEquals(value, resultValue);

        // ---------- 1 ----------
        result = actual.get(1);
        key = result.getFirst();
        value = result.getSecond();

        resultValue.clear();
        resultValue.addPrimitiveValue("value", "b\t3");
        assertEquals(key, resultKey);
        assertEquals(value, resultValue);

        // ---------- 2 ----------
        result = actual.get(2);
        key = result.getFirst();
        value = result.getSecond();

        resultValue.clear();
        resultValue.addPrimitiveValue("value", "c\t3");
        assertEquals(key, resultKey);
        assertEquals(value, resultValue);

        // ---------- 3 ----------
        result = actual.get(3);
        key = result.getFirst();
        value = result.getSecond();

        resultValue.clear();
        resultValue.addPrimitiveValue("value", "d\t3");
        assertEquals(key, resultKey);
        assertEquals(value, resultValue);

        // ========= key1 =========
        resultKey.clear();
        resultKey.addPrimitiveValue("key", "dcba");

        // ---------- 4 ----------
        result = actual.get(4);
        key = result.getFirst();
        value = result.getSecond();

        resultValue.clear();
        resultValue.addPrimitiveValue("value", "a\t1");
        assertEquals(key, resultKey);
        assertEquals(value, resultValue);

        // ---------- 5 ----------
        result = actual.get(5);
        key = result.getFirst();
        value = result.getSecond();

        resultValue.clear();
        resultValue.addPrimitiveValue("value", "b\t1");
        assertEquals(key, resultKey);
        assertEquals(value, resultValue);

        // ---------- 6 ----------
        result = actual.get(6);
        key = result.getFirst();
        value = result.getSecond();

        resultValue.clear();
        resultValue.addPrimitiveValue("value", "c\t1");
        assertEquals(key, resultKey);
        assertEquals(value, resultValue);

        // ---------- 7 ----------
        result = actual.get(7);
        key = result.getFirst();
        value = result.getSecond();

        resultValue.clear();
        resultValue.addPrimitiveValue("value", "d\t1");
        assertEquals(key, resultKey);
        assertEquals(value, resultValue);
    }
}
