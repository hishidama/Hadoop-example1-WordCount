package example1.afw.batchapp.afwWordcountBatch2.wordcount2.stage0001;
import com.asakusafw.runtime.core.Result;
import example1.afw.modelgen.dmdl.model.WordCountModel;
import example1.afw.modelgen.dmdl.model.WordModel;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
/**
 * {@code wm->WordCountOperator.count(operator#1681367116)}へのシャッフル処理を担当するプログラムの断片。
 */
@SuppressWarnings("deprecation") public final class MapOutputFragment1 implements Result<WordModel> {
    private final TaskInputOutputContext<?, ?, ? super ShuffleKey, ? super ShuffleValue> collector;
    private final ShuffleKey key = new ShuffleKey();
    private final ShuffleValue value = new ShuffleValue();
    private WordCountModel cache = new WordCountModel();
    /**
     * インスタンスを生成する。
     * @param collector 実際の出力先
     */
    public MapOutputFragment1(TaskInputOutputContext<?, ?, ? super ShuffleKey, ? super ShuffleValue> collector) {
        this.collector = collector;
    }
    @Override public void add(WordModel result) {
        this.cache.setWordOption(result.getWordOption());
        this.cache.getCountOption().modify(1L);
        this.key.setPort1(this.cache);
        this.value.setPort1(this.cache);
        try {
            this.collector.write(this.key, this.value);
        }
        catch(Exception exception) {
            throw new Result.OutputException(exception);
        }
    }
}