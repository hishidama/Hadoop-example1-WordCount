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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.huahinframework.core.io.Key;
import org.huahinframework.core.io.Value;
import org.junit.Test;

/**
 *
 */
public class KeyValueTest {
    private class KeyMapTestMapper extends Mapper<LongWritable, Text, Key, Value> {
        private Key emitKey = new Key();
        private Value emitValue = new Value();

        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException ,InterruptedException {
            String[] inputs = value.toString().split("\t");

            int cnt = 0;
            for (String s : inputs) {
                emitKey.addPrimitiveValue("key" + ++cnt, s);
                emitValue.addPrimitiveValue("value" + ++cnt, s);
            }

            context.write(emitKey, emitValue);
        }
    }

    private Mapper<LongWritable, Text, Key, Value> mapper;
    private MapDriver<LongWritable, Text, Key, Value> driver;

    @Test
    public void test() {
        Key key = new Key();
        key.addPrimitiveValue("key1", "aaa");
        key.addPrimitiveValue("key2", "bbb");
        key.addPrimitiveValue("key3", "ccc");

        Value value = new Value();
        value.addPrimitiveValue("value1", "aaa");
        value.addPrimitiveValue("value2", "bbb");
        value.addPrimitiveValue("value3", "ccc");

        mapper = new KeyMapTestMapper();
        driver = new MapDriver<LongWritable, Text, Key, Value>(mapper);
        driver.withInput(new LongWritable(1L), new Text("aaa\tbbb\tccc"));
        driver.withOutput(key, value);
        driver.runTest();
    }
}
