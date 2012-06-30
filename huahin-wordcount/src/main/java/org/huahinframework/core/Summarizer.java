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
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;
import org.huahinframework.core.io.Key;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.io.Value;

/**
 * This class is wrapping the {@link Reducer} class.
 * <code>Summarizer</code> will process a given record from {@link Filter}.
 *
 * <p>The framework first calls {@link #summarizerSetup()}, followed by
 * {@link #init()} and {@link #summarizer(Writer)} for each {@link Record} in the input.</p>
 *
 * <p>The following is an example to do a count of the WORD that was passed from Filter.
 * Has not specified grouping, will use the default.</p>
 *
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class WordSummarizer extends Summarizer {
 *   public void init() {
 *   }
 *
 *   public void summarizer(Writer writer)
 *       throws IOException, InterruptedException {
 *     int count = 0;
 *     while (hasNext()) {
 *       next(writer);
 *       count++;
 *     }
 *
 *     Record emitRecord = new Record();
 *     emitRecord.addValue("COUNT", count);
 *     writer.write(emitRecord);
 *   }
 *
 *   public void summarizerSetup() {
 *   }
 * }
 * </pre></blockquote></p>
 *
 * @see Record
 * @see Writer
 * @see Summarizer
 */
public abstract class Summarizer extends Reducer<Key, Value, Key, Value> {
    protected Context context;
    private Writer writer = new Writer();
    private Iterator<Value> recordIte;
    private Key currentKey;

    /**
     * {@inheritDoc}
     */
    public void reduce(Key key, Iterable<Value> values, Context context)
            throws IOException ,InterruptedException {
        writer.setContext(context);
        init();

        currentKey = key;
        recordIte = values.iterator();
        summarizer(writer);
    }

    /**
     * {@inheritDoc}
     */
    public void setup(Context context)
            throws IOException ,InterruptedException {
        this.context = context;
        summarizerSetup();
    }

    /**
     * Returns true if the iteration has more {@link Record}.
     * @return true if the iterator has more {@link Record}.
     */
    protected boolean hasNext() {
        return recordIte.hasNext();
    }

    /**
     * Returns the next {@link Record} in the iteration.
     * @param writer the output using the writer.
     * @return the next {@link Record} in the iteration.
     */
    protected Record next(Writer writer) {
        Record record = new Record(currentKey, recordIte.next());
        writer.setDefaultRecord(record);
        return record;
    }

    /**
     * This method is returns the record for get the grouping.
     * @return grouping record
     */
    protected Record getGroupingRecord() {
        Record record = new Record();
        record.setKey(currentKey);
        return record;
    }

    /**
     * Get Job String parameter
     * @param name parameter name
     * @return parameter value
     */
    protected String getStringParameter(String name) {
        return context.getConfiguration().get(name);
    }

    /**
     * Get Job Boolean parameter
     * @param name parameter name
     * @return parameter value
     */
    protected boolean getBooleanParameter(String name) {
        return context.getConfiguration().getBoolean(name, false);
    }

    /**
     * Get Job Integer parameter
     * @param name parameter name
     * @return parameterr value
     */
    protected int getIntParameter(String name) {
        return context.getConfiguration().getInt(name, -1);
    }

    /**
     * Will be called for each group.
     */
    public abstract void init();

    /**
     * <code>summarizer</code> will process a given record from {@link Filter}.
     * @param writer the output using the writer.
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract void summarizer(Writer writer) throws IOException ,InterruptedException;

    /**
     * Called once at the beginning of the task.
     */
    public abstract void summarizerSetup();
}
