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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.huahinframework.core.io.Key;
import org.junit.Test;

/**
 *
 */
public class KeyTest {
    private class KeyTestMapperText extends Mapper<LongWritable, Text, Key, LongWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException ,InterruptedException {
            String[] inputs = value.toString().split("\t");
            Key emitKey = new Key();
            for (String s : inputs) {
                emitKey.addPrimitiveValue("word", s);
            }

            context.write(emitKey, key);
        }
    }

    private class KeyTestMapperInt extends Mapper<LongWritable, Text, Key, LongWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException ,InterruptedException {
            String[] inputs = value.toString().split("\t");
            Key emitKey = new Key();
            for (String s : inputs) {
                emitKey.addPrimitiveValue("word", Integer.valueOf(s));
            }

            context.write(emitKey, key);
        }
    }

    private Mapper<LongWritable, Text, Key, LongWritable> mapper;
    private MapDriver<LongWritable, Text, Key, LongWritable> driver;

    @Test
    public void testKeyEquals() {
        Key key1 = new Key();
        key1.addPrimitiveValue("word", "foo");
        key1.addPrimitiveValue("count", 1);
        key1.addPrimitiveValue("value", "bar");

        Key key2 = new Key();
        key2.addPrimitiveValue("word", "foo");
        key2.addPrimitiveValue("count", 1);
        key2.addPrimitiveValue("value", "bar");

        Key key3 = new Key();
        key3.addPrimitiveValue("word", "foo");
        key3.addPrimitiveValue("count", 3);
        key3.addPrimitiveValue("value", "hoge");

        assertEquals(key1.hashCode(), key2.hashCode());
        assertEquals(key1.toString(), key2.toString());
        assertTrue(key1.equals(key2));
        assertEquals(key1.compareTo(key2), 0);

        assertFalse(key1.equals(key3));
        assertFalse(key1.compareTo(key3) == 0);
    }

    @Test
    public void testGetKey() {
        Key key = new Key();
        key.addPrimitiveValue("word", "foo");
        key.addPrimitiveValue("count", 1);
        key.addPrimitiveValue("value", "bar");

        assertEquals(key.getPrimitiveValue("word"), "foo");
        assertEquals(key.getPrimitiveValue("count"), 1);
        assertEquals(key.getPrimitiveValue("value"), "bar");
        assertEquals(key.getPrimitiveValue("foo"), null);
    }

    @Test
    public void testKeyMapperText() {
        Key key = new Key();
        key.addPrimitiveValue("word", "111");
        key.addPrimitiveValue("word", "222");
        key.addPrimitiveValue("word", "333");
        key.addPrimitiveValue("word", "444");

        mapper = new KeyTestMapperText();
        driver = new MapDriver<LongWritable, Text, Key, LongWritable>(mapper);
        driver.withInput(new LongWritable(1L), new Text("111\t222\t333\t444"));
        driver.withOutput(key, new LongWritable(1L));
        driver.runTest();
    }

    @Test
    public void testKeyMapperInt() {
        Key key = new Key();
        key.addPrimitiveValue("word", 111);
        key.addPrimitiveValue("word", 222);
        key.addPrimitiveValue("word", 333);
        key.addPrimitiveValue("word", 444);

        mapper = new KeyTestMapperInt();
        driver = new MapDriver<LongWritable, Text, Key, LongWritable>(mapper);
        driver.withInput(new LongWritable(1L), new Text("111\t222\t333\t444"));
        driver.withOutput(key, new LongWritable(1L));
        driver.runTest();
    }

    @Test
    public void testNull() {
        Key key = new Key();
        key.addPrimitiveValue("", null);
        assertEquals(key.toString(), "(null)");
    }
}
