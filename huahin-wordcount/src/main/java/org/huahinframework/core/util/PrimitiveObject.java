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

/**
 * <code>PrimitiveObject</code> has the Java primitive object.
 */
public class PrimitiveObject {
    private int type;
    private boolean array = false;
    private boolean map = false;
    private int arrayType;
    private int mapKeyType;
    private int mapValueType;
    private Object object;

    /**
     * default constractor
     */
    public PrimitiveObject() {
    }

    /**
     * @param type object type
     * @param object Java primitive object
     */
    public PrimitiveObject(int type, Object object) {
        this.type = type;
        this.object = object;
    }

    /**
     * @param type object type
     * @param array if true, set the array
     * @param arrayType array value type
     * @param object Java array object
     */
    public PrimitiveObject(int type, boolean array, int arrayType, Object object) {
        this.type = type;
        this.array = array;
        this.arrayType = arrayType;
        this.object = object;
    }

    /**
     * @param type object type
     * @param map if true, set the map
     * @param mapKeyType map key type
     * @param mapValueType map value type
     * @param object Java map object
     */
    public PrimitiveObject(int type, boolean map, int mapKeyType, int mapValueType, Object object) {
        this.type = type;
        this.map = map;
        this.mapKeyType = mapKeyType;
        this.mapValueType = mapValueType;
        this.object = object;
    }

    /**
     * @return the type
     */
    public int getType() {
        return type;
    }

    /**
     * @return the array
     */
    public boolean isArray() {
        return array;
    }

    /**
     * @return the map
     */
    public boolean isMap() {
        return map;
    }

    /**
     * @return the arrayType
     */
    public int getArrayType() {
        return arrayType;
    }

    /**
     * @return the mapKeyType
     */
    public int getMapKeyType() {
        return mapKeyType;
    }

    /**
     * @return the mapValueType
     */
    public int getMapValueType() {
        return mapValueType;
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
    public Object getObject() {
        return object;
    }

    /**
     * @param object the object to set
     */
    public void setObject(Object object) {
        this.object = object;
    }
}
