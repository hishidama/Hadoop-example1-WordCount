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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.hadoop.util.StringUtils;

/**
 * Is a utility for the strings.
 */
public class StringUtil {
    /**
     * comma: ','
     */
    public static final String COMMA = ",";

    /**
     * tab: '\t'
     */
    public static final String TAB = "\t";

    /**
     * job ID prefix
     */
    private static final String JOB_PREFIX = "JOB_";

    /**
     * split the given string. default separator commna. perform the trim.
     * @param str strings
     * @return the array of strings computed by splitting this string around matches of the given separator
     */
    public static String[] split(String str) {
        return split(str, COMMA, false);
    }

    /**
     * splits this string around matches of the given separator.
     * @param str strings
     * @param separator separator
     * @param trim include the last separator
     * @return the array of strings computed by splitting this string around matches of the given separator
     */
    public static String[] split(String str, String separator, boolean trim) {
        if (str == null) {
            return null;
        }

        char sep = separator.charAt(0);

        ArrayList<String> strList = new ArrayList<String>();
        StringBuilder split = new StringBuilder();
        int index = 0;
        while ((index = StringUtils.findNext(str, sep, StringUtils.ESCAPE_CHAR, index, split)) >= 0) {
            ++index; // move over the separator for next search
            strList.add(split.toString());
            split.setLength(0); // reset the buffer
        }

        strList.add(split.toString());
        // remove trailing empty split(s)
        if (trim) {
            int last = strList.size(); // last split
            while (--last>=0 && "".equals(strList.get(last))) {
                strList.remove(last);
            }
        }

        return strList.toArray(new String[strList.size()]);
    }

    /**
     * create internal job ID
     * @return job ID
     * @throws NoSuchAlgorithmException
     */
    public static String createInternalJobID()
            throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");

        byte[] data = UUID.randomUUID().toString().getBytes();
        md.update(data);

        byte[] digest = md.digest();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < digest.length; i++) {
          sb.append(Integer.toHexString(0xff & digest[i]));
        }

        return JOB_PREFIX + sb.toString().toUpperCase();
    }
}
