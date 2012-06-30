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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.huahinframework.core.util.HDFSUtils;
import org.huahinframework.core.util.PathUtils;
import org.huahinframework.core.util.StringUtil;

/**
 * Job tool class is to set the Job.
 *
 * <p>To configure the Job, you will create a tool that inherit from this class.
 * By default, the intermediate file path creation and deletion is done automatically.</p>
 *
 * <p>If you want to perform more advanced configuration, you can also edit the parameters directly.</p>
 *
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class RankingJobTool extends SimpleJobTool {
 *   protected String setInputPath(String[] args) {
 *     return args[0];
 *   }
 *   protected String setOutputPath(String[] args) {
 *     return args[1];
 *   }
 *   protected void setup() throws Exception {
 *     final String[] labels = new String[] { "USER", "DATE", "REFERER", "URL" };
 *
 *     // Labeled the first job
 *     SimpleJob job1 = addJob(labels, StringUtil.TAB);
 *     job1.setFilter(FirstFilter.class);
 *     job1.setSummaizer(FirstSummarizer.class);
 *
 *     // second job
 *     SimpleJob job2 = addJob();
 *     job2.setSummaizer(SecondSummarizer.class);
 *   }
 * }
 * </pre></blockquote></p>
 *
 * @see SimpleJob
 */
public abstract class SimpleJobTool extends Configured implements Tool {
    private static final String INTERMEDIATE_PATH = "%s-%s-intermediate-%d";
    protected String jobName;

    protected SequencalJobChain sequencalJobChain = new SequencalJobChain();

    protected Configuration conf;

    /**
     * input path
     */
    protected String input;

    /**
     * output path
     */
    protected String output;

    /**
     * List of intermediate file path
     */
    protected List<String> intermediatePaths = new ArrayList<String>();

    /**
     * Utility to set the path.
     * By default, create an instance in HDFSUtils operations of HDFS.
     * If you are in the Amazon Elastic MapReduce is used to create the instance is S3Utils.
     */
    protected PathUtils pathUtils;

    /**
     * Whether to automatically create the intermediate path multistage MpaReduce.
     */
    protected boolean autoIntermediatePath = true;

    /**
     * Whether you want to delete the intermediate file of multistage MpaReduce.
     */
    protected boolean deleteIntermediatePath = true;

    /**
     * {@inheritDoc}
     */
    @Override
    public int run(String[] args) throws Exception {
        conf = getConf();
        pathUtils = new HDFSUtils(conf);
        jobName = StringUtil.createInternalJobID();

        input = setInputPath(args);
        output = setOutputPath(args);
        setup();

        // Make the intermediate path
        if (autoIntermediatePath) {
            int stepNo = 1;
            Job lastJob = null;
            String lastIntermediatePath = null;
            for (Job j : sequencalJobChain.getJobs()) {
                if (lastJob == null) {
                    TextInputFormat.setInputPaths(j, input);
                    j.setInputFormatClass(TextInputFormat.class);
                } else {
                    SequenceFileInputFormat.setInputPaths(j, lastIntermediatePath);
                    j.setInputFormatClass(SequenceFileInputFormat.class);
                }

                lastIntermediatePath = String.format(INTERMEDIATE_PATH, output, jobName, stepNo);
                intermediatePaths.add(lastIntermediatePath);

                SequenceFileOutputFormat.setOutputPath(j, new Path(lastIntermediatePath));
                j.setOutputFormatClass(SequenceFileOutputFormat.class);

                stepNo++;
                lastJob = j;
            }

            for (Job j : sequencalJobChain.getJobs()) {
                if (j instanceof SimpleJob) {
                    SimpleJob jj = (SimpleJob) j;
                    if (!jj.isReducer()) {
                        jj.setNumReduceTasks(0);
                    }
                }
            }

            intermediatePaths.remove(lastIntermediatePath);
            TextOutputFormat.setOutputPath(lastJob, new Path(output));
            lastJob.setOutputFormatClass(TextOutputFormat.class);
        }

        // Running all jobs
        SequencalJobExecuteResults results = sequencalJobChain.runAll();

        // Delete the intermediate data
        if (deleteIntermediatePath) {
            for (String path : intermediatePaths) {
                pathUtils.delete(path);
            }
        }

        return results.isAllJobSuccessful() ? 0 : -1;
    }

    /**
     * Set the path from the input parameters.
     * @param args input args
     * @return input path
     */
    protected abstract String setInputPath(String[] args);

    /**
     * Set the path from the output parameters.
     * @param args output args
     * @return output path
     */
    protected abstract String setOutputPath(String[] args);

    /**
     * Sets of Job in this method.
     * @throws Exception
     */
    protected abstract void setup() throws Exception;

    /**
     * @return new {@link SimpleJob} class
     * @throws IOException
     */
    protected SimpleJob addJob() throws IOException {
        return addJob(new SimpleJob(conf, jobName), null, null, false);
    }

    /**
     * @param labels label of input data
     * @param separator separator of data
     * @return new {@link SimpleJob} class
     * @throws IOException
     */
    protected SimpleJob addJob(String[] labels, String separator) throws IOException {
        return addJob(new SimpleJob(conf, jobName), labels, separator, false);
    }

    /**
     * @param labels label of input data
     * @param separator separator of data
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     * @return new {@link SimpleJob} class
     * @throws IOException
     */
    protected SimpleJob addJob(String[] labels, String separator, boolean formatIgnored) throws IOException {
        return addJob(new SimpleJob(conf, jobName), labels, separator, formatIgnored);
    }

    /**
     * @param job new {@link SimpleJob}
     * @param labels label of input data
     * @param separator separator of data
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     * @return new {@link SimpleJob} class
     * @throws IOException
     */
    protected SimpleJob addJob(SimpleJob job, String[] labels, String separator, boolean formatIgnored) throws IOException {
        if (labels != null) {
            job.getConfiguration().setStrings(SimpleJob.LABELS, labels);
            job.getConfiguration().set(SimpleJob.SEPARATOR, separator);
        }

        job.setJarByClass(SimpleJobTool.class);
        job.setFirst(sequencalJobChain.isEmpty());
        job.setFormatIgnored(formatIgnored);
        sequencalJobChain.add(job);
        // TODO: How to do Reduce task.
//        job.setNumReduceTasks(1);
        return job;
    }
}
