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

import org.apache.hadoop.io.Writable;

/**
 * <code>HadoopObject</code> has the Writable of Hadoop.
 */
public class HadoopObject {
    private int type;
    private Writable object;

    /**
     * default constractor
     */
    public HadoopObject() {
    }

    /**
     * @param type object type
     * @param object Hadoop {@link Writable} object
     */
    public HadoopObject(int type, Writable object) {
        this.type = type;
        this.object = object;
    }

    /**
     * @return the type
     */
    public int getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(int type) {
        this.type = type;
    }

    /**
     * @return the object
     */
    public Writable getObject() {
        return object;
    }

    /**
     * @param object the object to set
     */
    public void setObject(Writable object) {
        this.object = object;
    }
}
