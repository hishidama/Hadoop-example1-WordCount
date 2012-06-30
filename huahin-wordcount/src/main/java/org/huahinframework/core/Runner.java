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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;

/**
 * Execution of the job can be done easily and use this class.
 *
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class Jobs {
 *   public static void main(String[] args) {
 *      String jobName = args[0];
 *      String[] newArgs = new String[args.length - 1];
 *      for (int i = 1; i < args.length; ++i) {
 *          newArgs[i - 1] = args[i];
 *      }
 *
 *     Runner runner = new Runner();
 *     runner.addJob("WordCount", WordCount.class);
 *     runner.addJob("Grep", Grep.class);
 *
 *     int status = runner.run(jobName, args);
 *
 *     System.exit(status);
 *   }
 * }
 * </pre></blockquote></p>
 */
public class Runner {
    private static final Log log = LogFactory.getLog(Runner.class);

    private Map<String, Class<? extends SimpleJobTool>> jobMap =
            new HashMap<String, Class<? extends SimpleJobTool>>();

    /**
     * Run the job.
     * @param jobName running job sequence name
     * @param args job parameters
     * @return job status
     */
    public int run(String jobName, String[] args) {
        int status = -1;

        try {
            status =  ToolRunner.run(jobMap.get(jobName).newInstance(), args);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
        }

        return status;
    }

    /**
     * Add job sequence.
     * @param name job sequence name
     * @param clazz SimpleJobTool class
     */
    public void addJob(String name, Class<? extends SimpleJobTool> clazz) {
        jobMap.put(name, clazz);
    }
}
