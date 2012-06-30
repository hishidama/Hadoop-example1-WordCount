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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.huahinframework.core.io.Key;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.io.Value;
import org.huahinframework.core.util.StringUtil;

/**
 * This class is wrapping the {@link Mapper} class.
 * Inherits the <code>Filter</code> class instead of the {@link Mapper} class.
 *
 * <p>Map a simplified process can use the <code>Filter</code> class instead of the <code>Mapper</code> class.
 * Using the filter method, the {@link Record} input is passed instead of the KEY and VALUE.</p>
 *
 * <p>The framework first calls {@link #filterSetup()}, followed by
 * {@link #init()} and {@link #filter(Record, Writer)} for each {@link Record} in the input.
 * {@link #init()} is called before the {@link #filter(Record, Writer)} is called.</p>
 *
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class WordFilter extends Filter {
 *   public void init() {
 *   }
 *
 *   public void filter(Record record, Writer writer)
 *       throws IOException, InterruptedException {
 *     String text = record.getValueString("TEXT");
 *     String[] strings = StringUtil.split(text, StringUtil.TAB, true);
 *     for (String s : strings) {
 *       Record emitRecord = new Record();
 *       emitRecord.addGrouping("WORD", s);
 *       emitRecord.addValue("NUMBER", 1);
 *       writer.write(emitRecord);
 *     }
 *   }
 *
 *   public void filterSetup() {
 *   }
 * }
 * </pre></blockquote></p>
 *
 * @see Record
 * @see Writer
 * @see Summarizer
 */
public abstract class Filter extends Mapper<Writable, Writable, Key, Value> {
    protected Context context;
    private Writer writer = new Writer();
    private String[] labels;
    private String separator;
    private boolean first;
    private boolean formatIgnored;

    /**
     * {@inheritDoc}
     */
    public void map(Writable key, Writable value, Context context)
            throws IOException,InterruptedException {
        writer.setContext(context);
        init();

        Record record = new Record();
        if (first) {
            LongWritable k = (LongWritable) key;
            Text v = (Text) value;

            record.addGrouping("KEY", k.get());

            String[] strings = StringUtil.split(v.toString(), separator, false);
            if (labels.length != strings.length) {
                if (formatIgnored) {
                    throw new DataFormatException("input format error: " +
                                                  "label.length = " + labels.length +
                                                  "input.lenght = " + strings.length);
                }

                return;
            }

            for (int i = 0; i < strings.length; i++) {
                record.addValue(labels[i], strings[i]);
            }
        } else {
            record.setKey((Key) key);
            record.setValue((Value) value);
        }

        writer.setDefaultRecord(record);
        filter(record, writer);
    }

    /**
     * {@inheritDoc}
     */
    public void setup(Context context)
            throws IOException ,InterruptedException {
        this.context = context;
        first = context.getConfiguration().getBoolean(SimpleJob.FIRST, false);
        formatIgnored = context.getConfiguration().getBoolean(SimpleJob.FORMAT_IGNORED, false);
        labels = context.getConfiguration().getStrings(SimpleJob.LABELS);
        separator = context.getConfiguration().get(SimpleJob.SEPARATOR, StringUtil.COMMA);
        filterSetup();
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
     * Will be called before each filter is called.
     */
    public abstract void init();

    /**
     * <code>filter</code> for each {@link Record} in the input.
     * @param record input record
     * @param writer the output using the writer.
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract void filter(Record record, Writer writer) throws IOException ,InterruptedException;

    /**
     * Called once at the beginning of the task.
     */
    public abstract void filterSetup();
}
