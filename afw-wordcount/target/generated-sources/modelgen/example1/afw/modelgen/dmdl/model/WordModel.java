package example1.afw.modelgen.dmdl.model;
import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.model.DataModelKind;
import com.asakusafw.runtime.model.ModelInputLocation;
import com.asakusafw.runtime.model.ModelOutputLocation;
import com.asakusafw.runtime.model.PropertyOrder;
import com.asakusafw.runtime.value.StringOption;
import com.asakusafw.vocabulary.bulkloader.ColumnOrder;
import com.asakusafw.vocabulary.bulkloader.OriginalName;
import example1.afw.modelgen.dmdl.io.WordModelInput;
import example1.afw.modelgen.dmdl.io.WordModelOutput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/**
 * word_modelを表すデータモデルクラス。
 */
@ColumnOrder(value = {"WORD"})@DataModelKind("DMDL")@ModelInputLocation(WordModelInput.class)@ModelOutputLocation(
        WordModelOutput.class)@OriginalName(value = "WORD_MODEL")@PropertyOrder({"word"}) public class WordModel 
        implements DataModel<WordModel>, Writable {
    private final StringOption word = new StringOption();
    @Override@SuppressWarnings("deprecation") public void reset() {
        this.word.setNull();
    }
    @Override@SuppressWarnings("deprecation") public void copyFrom(WordModel other) {
        this.word.copyFrom(other.word);
    }
    /**
     * wordを返す。
     * @return word
     * @throws NullPointerException wordの値が<code>null</code>である場合
     */
    public Text getWord() {
        return this.word.get();
    }
    /**
     * wordを設定する。
     * @param value 設定する値
     */
    @SuppressWarnings("deprecation") public void setWord(Text value) {
        this.word.modify(value);
    }
    /**
     * <code>null</code>を許すwordを返す。
     * @return word
     */
    public StringOption getWordOption() {
        return this.word;
    }
    /**
     * wordを設定する。
     * @param option 設定する値、<code>null</code>の場合にはこのプロパティが<code>null</code>を表すようになる
     */
    @SuppressWarnings("deprecation") public void setWordOption(StringOption option) {
        this.word.copyFrom(option);
    }
    @Override public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("{");
        result.append("class=word_model");
        result.append(", word=");
        result.append(this.word);
        result.append("}");
        return result.toString();
    }
    @Override public int hashCode() {
        int prime = 31;
        int result = 1;
        result = prime * result + word.hashCode();
        return result;
    }
    @Override public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }
        if(obj == null) {
            return false;
        }
        if(this.getClass()!= obj.getClass()) {
            return false;
        }
        WordModel other = (WordModel) obj;
        if(this.word.equals(other.word)== false) {
            return false;
        }
        return true;
    }
    /**
     * wordを返す。
     * @return word
     * @throws NullPointerException wordの値が<code>null</code>である場合
     */
    public String getWordAsString() {
        return this.word.getAsString();
    }
    /**
     * wordを設定する。
     * @param word0 設定する値
     */
    @SuppressWarnings("deprecation") public void setWordAsString(String word0) {
        this.word.modify(word0);
    }
    @Override public void write(DataOutput out) throws IOException {
        word.write(out);
    }
    @Override public void readFields(DataInput in) throws IOException {
        word.readFields(in);
    }
}