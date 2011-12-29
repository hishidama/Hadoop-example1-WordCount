package test.jobflow.bid.wordcount.fileio.afwout.epilogue.result;
import com.asakusafw.runtime.stage.collector.SlotDistributor;
import com.asakusafw.runtime.stage.collector.SortableSlot;
import example1.afw.modelgen.dmdl.model.WordCountModel;
import java.io.IOException;
/**
 * 出力"result"に対するエピローグ用のマッパー。
 */
public class StageMapper0 extends SlotDistributor<WordCountModel> {
    @Override protected void setSlotSpec(WordCountModel value, SortableSlot slot) throws IOException {
        slot.begin(0);
        slot.addRandom();
    }
}