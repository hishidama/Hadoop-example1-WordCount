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
package org.huahinframework.core.lib.partition;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.huahinframework.core.io.Key;

/**
 * <code>SimplePartitioner</code> partitioner key is that you specify in the <code>Record#addGrouping</code>.
 */
public class SimplePartitioner extends Partitioner<Key, Writable> {
    /**
     * {@inheritDoc}
     */
    @Override
    public int getPartition(Key key, Writable value, int numPartitions) {
        return Math.abs(key.identifier().hashCode()) % numPartitions;
    }
}
