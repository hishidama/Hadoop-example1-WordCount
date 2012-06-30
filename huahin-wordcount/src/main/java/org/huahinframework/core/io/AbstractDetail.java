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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * This abstract class is used to configure the details of the value.
 */
public abstract class AbstractDetail<T> implements Detail<T> {
    protected IntWritable order = new IntWritable();
    protected Text label = new Text();

    /**
     * {@inheritDoc}
     */
    public int getOrder() {
        return order.get();
    }

    /**
     * @param order new order
     */
    public void setOrder(int order) {
        this.order.set(order);
    }

    /**
     * {@inheritDoc}
     */
    public String getLabel() {
        return label.toString();
    }
}
