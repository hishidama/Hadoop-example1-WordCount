package test.jobflow.bid.wordcount2.stage0001;
import com.asakusafw.runtime.core.Result;
import example1.afw.modelgen.dmdl.model.TextModel;
import example1.afw.modelgen.dmdl.model.WordModel;
import example1.afw.operator.WordCountOperatorImpl;
/**
 * {@code [tm->WordCountOperator.split(operator#172930161)]}の処理を担当するマッププログラムの断片。
 */
@SuppressWarnings("deprecation") public final class MapFragment1 implements Result<TextModel> {
    private final Result<WordModel> out;
    private WordCountOperatorImpl op = new WordCountOperatorImpl();
    /**
     * インスタンスを生成する。
     * @param out{@code WordCountOperator.split#out}への出力
     */
    public MapFragment1(Result<WordModel> out) {
        this.out = out;
    }
    @Override public void add(TextModel result) {
        this.op.split(result, this.out);
    }
}