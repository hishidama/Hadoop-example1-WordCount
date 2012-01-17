#!/bin/bash

### Move to the working directory
echo "Moving to ''$(dirname $(dirname $0))''"
pushd $(dirname $(dirname $0)) > /dev/null

### Batch - afwWordcountBatch2
echo "Processing batch "afwWordcountBatch2""

## Jobflow - wordcount2
echo "Processing jobflow 'wordcount2'"
# Initialize this jobflow
_EXECUTION_ID=$(uuidgen)
_BATCH_ID="afwWordcountBatch2"
_FLOW_ID="wordcount2"
echo "_EXECUTION_ID=$_EXECUTION_ID"
echo "_BATCH_ID=$_BATCH_ID"
echo "_FLOW_ID=$_FLOW_ID"

# Deploy this jobflow
echo "Deploying 'lib/jobflow-wordcount2.jar' into '$ASAKUSA_HOME/batchapps/$_BATCH_ID/lib'"
mkdir -p "$ASAKUSA_HOME/batchapps/$_BATCH_ID/lib"
cp "lib"/"jobflow-wordcount2.jar" "$ASAKUSA_HOME/batchapps/$_BATCH_ID/lib"

# Hadoop Stage - example1.afw.batchapp.afwWordcountBatch2.wordcount2.stage0001.StageClient
echo "Processing hadoop job 'afwWordcountBatch2.wordcount2.stage0001'"
pushd "$ASAKUSA_HOME" > /dev/null
experimental/bin/hadoop_job_run.sh "example1.afw.batchapp.afwWordcountBatch2.wordcount2.stage0001.StageClient" "$ASAKUSA_HOME/batchapps/$_BATCH_ID/lib"/"jobflow-wordcount2.jar" -D "com.asakusafw.executionId"="$_EXECUTION_ID" -D "com.asakusafw.user"="$USER" -D "com.asakusafw.batchArgs"="$ASAKUSA_BATCH_ARGS" $EXPERIMENTAL_OPTS
_RET=$?
popd > /dev/null
if [ $_RET -ne 0 ]; then
    echo "Invalid return code=$_RET, from 'experimental/bin/hadoop_job_run.sh "example1.afw.batchapp.afwWordcountBatch2.wordcount2.stage0001.StageClient" "$ASAKUSA_HOME/batchapps/$_BATCH_ID/lib"/"jobflow-wordcount2.jar" -D "com.asakusafw.executionId"="$_EXECUTION_ID" -D "com.asakusafw.user"="$USER" -D "com.asakusafw.batchArgs"="$ASAKUSA_BATCH_ARGS" $EXPERIMENTAL_OPTS'"
    echo "Finished: FAILURE"
    popd > /dev/null
    exit "$_RET"
fi

# Hadoop Stage - example1.afw.batchapp.afwWordcountBatch2.wordcount2.fileio.afwout.epilogue.StageClient
echo "Processing hadoop job 'afwWordcountBatch2.wordcount2.epilogue.fileio.afwout'"
pushd "$ASAKUSA_HOME" > /dev/null
experimental/bin/hadoop_job_run.sh "example1.afw.batchapp.afwWordcountBatch2.wordcount2.fileio.afwout.epilogue.StageClient" "$ASAKUSA_HOME/batchapps/$_BATCH_ID/lib"/"jobflow-wordcount2.jar" -D "com.asakusafw.executionId"="$_EXECUTION_ID" -D "com.asakusafw.user"="$USER" -D "com.asakusafw.batchArgs"="$ASAKUSA_BATCH_ARGS" $EXPERIMENTAL_OPTS
_RET=$?
popd > /dev/null
if [ $_RET -ne 0 ]; then
    echo "Invalid return code=$_RET, from 'experimental/bin/hadoop_job_run.sh "example1.afw.batchapp.afwWordcountBatch2.wordcount2.fileio.afwout.epilogue.StageClient" "$ASAKUSA_HOME/batchapps/$_BATCH_ID/lib"/"jobflow-wordcount2.jar" -D "com.asakusafw.executionId"="$_EXECUTION_ID" -D "com.asakusafw.user"="$USER" -D "com.asakusafw.batchArgs"="$ASAKUSA_BATCH_ARGS" $EXPERIMENTAL_OPTS'"
    echo "Finished: FAILURE"
    popd > /dev/null
    exit "$_RET"
fi

# Cleaner
echo "cleaning job temporary resources"
"$ASAKUSA_HOME/experimental/bin/clean_hadoop_work.sh" "target/hadoopwork/$_EXECUTION_ID" "afwWordcountBatch2" "wordcount2" "$_EXECUTION_ID" "$ASAKUSA_BATCH_ARGS"
_RET=$?
if [ $_RET -ne 0 ]; then
    echo "WARNING: Invalid return code=$_RET, from cleaner 'target/hadoopwork/$_EXECUTION_ID'"
fi

### Return to the original directory
echo "Moving back to the original directory"
popd > /dev/null

echo "Finished: SUCCESS"
