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

import java.util.ArrayList;
import java.util.List;

/**
 * List of {@link SequencalJobExecuteResult} class.
 */
public class SequencalJobExecuteResults {
    List<SequencalJobExecuteResult> results = new ArrayList<SequencalJobExecuteResult>();

    /**
     * Add {@link SequencalJobExecuteResult}
     * @param sequencalJobExecuteResult
     */
    public void add(SequencalJobExecuteResult sequencalJobExecuteResult) {
        this.results.add(sequencalJobExecuteResult);
    }

    /**
     * Returns true if this all job status. If false, returns false even one.
     * @return true if this all job status
     */
    public boolean isAllJobSuccessful() {
        for (SequencalJobExecuteResult result : results) {
            if (!result.isSuccessful()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns text of job name and job status.
     * @return text of job name and job status.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (SequencalJobExecuteResult result : results) {
            builder.append(result.getJobName()).append("\t").append(result.isSuccessful()).append("\n");
        }
        return builder.toString();
    }
}
