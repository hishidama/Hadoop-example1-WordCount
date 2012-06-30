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

/**
 * Set job execute result.
 */
public class SequencalJobExecuteResult {
    /**
     * <code>SequencalJobExecuteResult</code> status.
     * <p>DEFAULT - default</p>
     * <p>SUCCESS - success</p>
     * <p>FAIL - fail</p>
     */
    public enum State {
        DEFAULT, SUCCESS, FAIL;
    }

    private State state = State.DEFAULT;
    private String jobName;

    /**
     * @param isSuccessful - result job stauts
     * @param jobName - result job name
     */
    public SequencalJobExecuteResult(boolean isSuccessful, String jobName) {
        if (isSuccessful) {
            this.state = State.SUCCESS;
        } else {
            this.state = State.FAIL;
        }
        this.jobName = jobName;
    }

    /**
     * Returns true if this job sucess
     * @return true if this job sucess
     */
    public boolean isSuccessful() {
        return this.state == State.SUCCESS;
    }

    /**
     * Returns result job name
     * @return job name
     */
    public String getJobName() {
        return jobName;
    }
}
