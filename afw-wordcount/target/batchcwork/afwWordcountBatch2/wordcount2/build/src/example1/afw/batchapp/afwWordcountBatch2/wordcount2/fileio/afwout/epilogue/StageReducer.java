package example1.afw.batchapp.afwWordcountBatch2.wordcount2.fileio.afwout.epilogue;
import com.asakusafw.runtime.stage.collector.SlotSorter;
import example1.afw.modelgen.dmdl.model.WordCountModel;
import org.apache.hadoop.io.Writable;
/**
 * エピローグ用のレデューサー。
 */
public class StageReducer extends SlotSorter {
    @Override protected String[] getOutputNames() {
        String[] results = new String[1];
        results[0]= "result";
        return results;
    }
    @Override protected Writable[] createSlotObjects() {
        Writable[] results = new Writable[1];
        results[0]= new WordCountModel();
        return results;
    }
}