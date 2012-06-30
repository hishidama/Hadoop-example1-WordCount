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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Is a utility for the HDFS.
 */
public class HDFSUtils implements PathUtils {
    private Configuration conf;

    /**
     * @param conf {@link Configuration}
     */
    public HDFSUtils(Configuration conf) {
        this.conf = conf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fstatuss = fs.listStatus(new Path(path));
        if (fstatuss != null) {
            for (FileStatus fstatus : fstatuss) {
                fs.delete(fstatus.getPath(), true);
            }
        }
        fs.delete(new Path(path), true);
    }
}
