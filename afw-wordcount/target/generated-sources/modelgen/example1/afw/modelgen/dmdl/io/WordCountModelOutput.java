package example1.afw.modelgen.dmdl.io;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.io.RecordEmitter;
import example1.afw.modelgen.dmdl.model.WordCountModel;
import java.io.IOException;
/**
 * <code>word_count_model</code>をTSVなどのレコード形式で出力する。
 */
public final class WordCountModelOutput implements ModelOutput<WordCountModel> {
    private final RecordEmitter emitter;
    /**
     * インスタンスを生成する。
     * @param emitter 利用するエミッター
     * @throws IllegalArgumentException 引数にnullが指定された場合
     */
    public WordCountModelOutput(RecordEmitter emitter) {
        if(emitter == null) {
            throw new IllegalArgumentException();
        }
        this.emitter = emitter;
    }
    @Override public void write(WordCountModel model) throws IOException {
        emitter.emit(model.getWordOption());
        emitter.emit(model.getCountOption());
        emitter.endRecord();
    }
    @Override public void close() throws IOException {
        emitter.close();
    }
}