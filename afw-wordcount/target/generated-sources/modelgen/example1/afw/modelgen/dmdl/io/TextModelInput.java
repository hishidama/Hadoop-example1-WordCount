package example1.afw.modelgen.dmdl.io;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.RecordParser;
import example1.afw.modelgen.dmdl.model.TextModel;
import java.io.IOException;
/**
 * TSVファイルなどのレコードを表すファイルを入力として<code>text_model</code>を読み出す
 */
public final class TextModelInput implements ModelInput<TextModel> {
    private final RecordParser parser;
    /**
     * インスタンスを生成する。
     * @param parser 利用するパーサー
     * @throws IllegalArgumentException 引数に<code>null</code>が指定された場合
     */
    public TextModelInput(RecordParser parser) {
        if(parser == null) {
            throw new IllegalArgumentException("parser");
        }
        this.parser = parser;
    }
    @Override public boolean readTo(TextModel model) throws IOException {
        if(parser.next()== false) {
            return false;
        }
        parser.fill(model.getTextOption());
        return true;
    }
    @Override public void close() throws IOException {
        parser.close();
    }
}