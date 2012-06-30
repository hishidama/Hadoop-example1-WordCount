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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.huahinframework.core.io.Key;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.io.Value;
import org.huahinframework.core.lib.partition.SimpleGroupingComparator;
import org.huahinframework.core.lib.partition.SimpleSortComparator;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class SecondarySortUpperTest {
    private class KeyValueMapTestMapper extends Mapper<LongWritable, Text, Key, Value> {
        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException ,InterruptedException {
            Key emitKey = new Key();
            Value emitValue = new Value();

            String[] inputs = value.toString().split("\t");
            String domain = inputs[0];
            String uu = inputs[1];
            String pv = inputs[2];

            emitKey.addPrimitiveValue("domain",domain);
            emitKey.addPrimitiveValue("uu", uu, Record.SORT_UPPER, 1);
            emitKey.addPrimitiveValue("pv", pv, Record.SORT_UPPER, 2);

            emitValue.addPrimitiveValue("uu", uu);
            emitValue.addPrimitiveValue("pv", pv);

            context.write(emitKey, emitValue);
        }
    }

    private class KeyValueMapTestReducer extends Reducer<Key, Value, Key, Value> {
        public void reduce(Key key, Iterable<Value> values, Context context)
                throws java.io.IOException ,InterruptedException {
            for (Value value : values) {
                context.write(key, value);
            }
        }
    }

    private MapReduceDriver<LongWritable, Text, Key, Value, Key, Value> driver;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        driver =
                MapReduceDriver.newMapReduceDriver(new KeyValueMapTestMapper(),
                                                   new KeyValueMapTestReducer())
                               .withKeyGroupingComparator(new SimpleGroupingComparator())
                               .withKeyOrderComparator(new SimpleSortComparator());
    }

    @Test
    public void test() throws IOException {
        driver.addInput(new LongWritable(1L), new Text("domain\t10\t200"));
        driver.addInput(new LongWritable(1L), new Text("domain\t20\t100"));
        driver.addInput(new LongWritable(1L), new Text("domain\t10\t300"));
        driver.addInput(new LongWritable(1L), new Text("domain\t50\t500"));

        List<Pair<Key, Value>> actual = driver.run();
        for (Pair<Key, Value> pair : actual) {
            System.out.println(pair);
        }
        assertEquals(4, actual.size());

        Pair<Key, Value> result;
        Key resultKey;
        Value resultValue;

        Key key = new Key();
        Value value = new Value();

        // ========= key0 =========
        key.clear();
        key.addPrimitiveValue("domain", "domain");
        key.addPrimitiveValue("uu", 50, Record.SORT_UPPER, 1);
        key.addPrimitiveValue("pv", 500, Record.SORT_UPPER, 2);

        // ---------- 0 ----------
        result = actual.get(0);
        resultKey = result.getFirst();
        resultValue = result.getSecond();

        value.clear();
        value.addPrimitiveValue("uu", "50");
        value.addPrimitiveValue("pv", "500");

        assertEquals(key, resultKey);
        assertEquals(value, resultValue);

        // ========= key1 =========
        key.clear();
        key.addPrimitiveValue("domain", "domain");
        key.addPrimitiveValue("uu", 20, Record.SORT_UPPER, 1);
        key.addPrimitiveValue("pv", 100, Record.SORT_UPPER, 2);

        // ---------- 1 ----------
        result = actual.get(1);
        resultKey = result.getFirst();
        resultValue = result.getSecond();

        value.clear();
        value.addPrimitiveValue("uu", "20");
        value.addPrimitiveValue("pv", "100");

        assertEquals(key, resultKey);
        assertEquals(value, resultValue);

        // ========= key2 =========
        key.clear();
        key.addPrimitiveValue("domain", "domain");
        key.addPrimitiveValue("uu", 10, Record.SORT_UPPER, 1);
        key.addPrimitiveValue("pv", 300, Record.SORT_UPPER, 2);

        // ---------- 2 ----------
        result = actual.get(2);
        resultKey = result.getFirst();
        resultValue = result.getSecond();

        value.clear();
        value.addPrimitiveValue("uu", "10");
        value.addPrimitiveValue("pv", "300");

        assertEquals(key, resultKey);
        assertEquals(value, resultValue);

        // ========= key3 =========
        key.clear();
        key.addPrimitiveValue("domain", "domain");
        key.addPrimitiveValue("uu", 10, Record.SORT_UPPER, 1);
        key.addPrimitiveValue("pv", 200, Record.SORT_UPPER, 2);

        // ---------- 2 ----------
        result = actual.get(3);
        resultKey = result.getFirst();
        resultValue = result.getSecond();

        value.clear();
        value.addPrimitiveValue("uu", "10");
        value.addPrimitiveValue("pv", "200");

        assertEquals(key, resultKey);
        assertEquals(value, resultValue);
    }
}
