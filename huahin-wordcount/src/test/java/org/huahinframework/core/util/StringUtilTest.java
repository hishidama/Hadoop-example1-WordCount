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

import static org.junit.Assert.*;

import org.huahinframework.core.util.StringUtil;
import org.junit.Test;

/**
 *
 */
public class StringUtilTest {
    private static final String STR = "aaa\tbbb\tccc\tddd\t";
    private static final String[] TRIM = new String[] { "aaa", "bbb", "ccc", "ddd" };
    private static final String[] NOT_TRIM = new String[] { "aaa", "bbb", "ccc", "ddd", "" };

    @Test
    public void testTrim() {
        String[] ss = StringUtil.split(STR, StringUtil.TAB, true);
        assertArrayEquals(ss, TRIM);
    }

    @Test
    public void testNotTrim() {
        String[] ss = StringUtil.split(STR, StringUtil.TAB, false);
        assertArrayEquals(ss, NOT_TRIM);
    }
}
