package example1.afw.modelgen.dmdl.io;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.RecordParser;
import example1.afw.modelgen.dmdl.model.WordCountModel;
import java.io.IOException;
/**
 * TSVファイルなどのレコードを表すファイルを入力として<code>word_count_model</code>を読み出す
 */
public final class WordCountModelInput implements ModelInput<WordCountModel> {
    private final RecordParser parser;
    /**
     * インスタンスを生成する。
     * @param parser 利用するパーサー
     * @throws IllegalArgumentException 引数に<code>null</code>が指定された場合
     */
    public WordCountModelInput(RecordParser parser) {
        if(parser == null) {
            throw new IllegalArgumentException("parser");
        }
        this.parser = parser;
    }
    @Override public boolean readTo(WordCountModel model) throws IOException {
        if(parser.next()== false) {
            return false;
        }
        parser.fill(model.getWordOption());
        parser.fill(model.getCountOption());
        return true;
    }
    @Override public void close() throws IOException {
        parser.close();
    }
}