package example1.afw.modelgen.dmdl.io;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.io.RecordEmitter;
import example1.afw.modelgen.dmdl.model.TextModel;
import java.io.IOException;
/**
 * <code>text_model</code>をTSVなどのレコード形式で出力する。
 */
public final class TextModelOutput implements ModelOutput<TextModel> {
    private final RecordEmitter emitter;
    /**
     * インスタンスを生成する。
     * @param emitter 利用するエミッター
     * @throws IllegalArgumentException 引数にnullが指定された場合
     */
    public TextModelOutput(RecordEmitter emitter) {
        if(emitter == null) {
            throw new IllegalArgumentException();
        }
        this.emitter = emitter;
    }
    @Override public void write(TextModel model) throws IOException {
        emitter.emit(model.getTextOption());
        emitter.endRecord();
    }
    @Override public void close() throws IOException {
        emitter.close();
    }
}