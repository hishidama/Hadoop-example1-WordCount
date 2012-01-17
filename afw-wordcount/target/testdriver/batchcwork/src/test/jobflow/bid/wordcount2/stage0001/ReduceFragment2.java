package test.jobflow.bid.wordcount2.stage0001;
import com.asakusafw.runtime.core.Result;
import example1.afw.modelgen.dmdl.model.WordCountModel;
/**
 * {@code [wm->WordCountOperator.count(operator#1849015814)]}の処理を担当するマッププログラムの断片。
 */
@SuppressWarnings("deprecation") public final class ReduceFragment2 extends com.asakusafw.runtime.flow.Rendezvous<
        ShuffleValue> {
    private final Result<WordCountModel> out;
    private boolean initialized;
    private WordCountModel cache = new WordCountModel();
    /**
     * インスタンスを生成する。
     * @param out{@code WordCountOperator.count#out}への出力
     */
    public ReduceFragment2(Result<WordCountModel> out) {
        this.out = out;
    }
    @Override public void process(ShuffleValue value) {
        switch(value.getSegmentId()) {
            case 1:
                this.process0001(value.getPort1());
                break;
            default:
                throw new AssertionError(value);
        }
    }
    @Override public void begin() {
        this.initialized = false;
    }
    @Override public void end() {
        this.out.add(this.cache);
    }
    private void process0001(WordCountModel value) {
        if(this.initialized) {
            this.cache.getCountOption().add(value.getCountOption());
        }
        else {
            this.cache.copyFrom(value);
            this.initialized = true;
        }
    }
}