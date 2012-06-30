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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapreduce.Job;

/**
 * Run all of the job has been added.
 */
public class SequencalJobChain {
    private List<Job> jobs = new ArrayList<Job>();

    /**
     * Add job
     * @param job
     */
    public void add(Job job) {
        this.jobs.add(job);
    }

    /**
     * Add jobs
     * @param jobs
     */
    public void add(Job... jobs) {
        this.jobs.addAll(Arrays.asList(jobs));
    }

    /**
     * Returns true if this jobs no elements.
     * @return true if this jobs no elements
     */
    public boolean isEmpty() {
        return jobs.isEmpty();
    }

    /**
     * Returns jobs list
     * @return jobs list
     */
    public List<Job> getJobs() {
        return jobs;
    }

    /**
     * Run all of the job has been added.
     * @return all jobs status
     */
    public SequencalJobExecuteResults runAll() {
        SequencalJobExecuteResults results = new SequencalJobExecuteResults();
        for (Job job : jobs) {
            job.setJarByClass(SequencalJobChain.class);
            try {
                boolean isSuccessful = job.waitForCompletion(true);
                results.add(new SequencalJobExecuteResult(isSuccessful, job.getJobName()));
                if (!isSuccessful) {
                    break;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return results;
    }
}
