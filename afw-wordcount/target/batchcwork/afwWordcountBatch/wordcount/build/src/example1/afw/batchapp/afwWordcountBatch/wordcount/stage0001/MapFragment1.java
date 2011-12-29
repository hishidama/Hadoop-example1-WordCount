package example1.afw.batchapp.afwWordcountBatch.wordcount.stage0001;
import com.asakusafw.runtime.core.Result;
import example1.afw.modelgen.dmdl.model.WordModel;
/**
 * {@code [in->padding(pseud#971277631)]}の処理を担当するマッププログラムの断片。
 */
@SuppressWarnings("deprecation") public final class MapFragment1 implements Result<WordModel> {
    private final Result<WordModel> out;
    /**
     * インスタンスを生成する。
     * @param out{@code padding#out}への出力
     */
    public MapFragment1(Result<WordModel> out) {
        this.out = out;
    }
    @Override public void add(WordModel result) {
        this.out.add(result);
    }
}