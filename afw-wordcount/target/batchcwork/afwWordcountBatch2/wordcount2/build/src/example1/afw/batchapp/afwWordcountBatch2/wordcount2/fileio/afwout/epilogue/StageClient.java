package example1.afw.batchapp.afwWordcountBatch2.wordcount2.fileio.afwout.epilogue;
import com.asakusafw.runtime.stage.AbstractStageClient;
import com.asakusafw.runtime.stage.StageInput;
import com.asakusafw.runtime.stage.StageOutput;
import com.asakusafw.runtime.stage.collector.SortableSlot;
import com.asakusafw.runtime.stage.collector.WritableSlot;
import example1.afw.batchapp.afwWordcountBatch2.wordcount2.fileio.afwout.epilogue.result.StageMapper0;
import example1.afw.jobflow.WordCountModelOutputFormat;
import example1.afw.modelgen.dmdl.model.WordCountModel;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
/**
 * "fileio.afwout"のエピローグステージのジョブを実行するクライアント。
 */
public final class StageClient extends AbstractStageClient {
    @Override protected String getBatchId() {
        return "afwWordcountBatch2";
    }
    @Override protected String getFlowId() {
        return "wordcount2";
    }
    @Override protected String getStageId() {
        return "epilogue.fileio.afwout";
    }
    @Override protected String getStageOutputPath() {
        return "example1/afwout";
    }
    @Override protected List<StageInput> getStageInputs() {
        List<StageInput> results = new ArrayList<StageInput>();
        results.add(new StageInput("target/hadoopwork/${execution_id}/afwWordcountBatch2/wordcount2/stage0001/result0-*"
                , SequenceFileInputFormat.class, StageMapper0.class));
        return results;
    }
    @Override protected List<StageOutput> getStageOutputs() {
        List<StageOutput> results = new ArrayList<StageOutput>();
        results.add(new StageOutput("result", NullWritable.class, WordCountModel.class, WordCountModelOutputFormat.class
                ));
        return results;
    }
    @Override protected Class<SortableSlot> getShuffleKeyClassOrNull() {
        return SortableSlot.class;
    }
    @Override protected Class<WritableSlot> getShuffleValueClassOrNull() {
        return WritableSlot.class;
    }
    @Override protected Class<SortableSlot.Partitioner> getPartitionerClassOrNull() {
        return SortableSlot.Partitioner.class;
    }
    @Override protected Class<StageReducer> getReducerClassOrNull() {
        return StageReducer.class;
    }
}