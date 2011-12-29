package example1.afw.modelgen.dmdl.model;
import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.model.DataModelKind;
import com.asakusafw.runtime.model.ModelInputLocation;
import com.asakusafw.runtime.model.ModelOutputLocation;
import com.asakusafw.runtime.model.PropertyOrder;
import com.asakusafw.runtime.value.LongOption;
import com.asakusafw.runtime.value.StringOption;
import com.asakusafw.vocabulary.bulkloader.ColumnOrder;
import com.asakusafw.vocabulary.bulkloader.OriginalName;
import com.asakusafw.vocabulary.model.Key;
import com.asakusafw.vocabulary.model.Summarized;
import example1.afw.modelgen.dmdl.io.WordCountModelInput;
import example1.afw.modelgen.dmdl.io.WordCountModelOutput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/**
 * word_count_modelを表すデータモデルクラス。
 */
@ColumnOrder(value = {"WORD", "COUNT"})@DataModelKind("DMDL")@ModelInputLocation(WordCountModelInput.class)@
        ModelOutputLocation(WordCountModelOutput.class)@OriginalName(value = "WORD_COUNT_MODEL")@PropertyOrder({"word", 
            "count"})@Summarized(term = @Summarized.Term(source = WordModel.class, foldings = {@Summarized.Folding(
            aggregator = Summarized.Aggregator.ANY, source = "word", destination = "word"),@Summarized.Folding(
            aggregator = Summarized.Aggregator.COUNT, source = "word", destination = "count")}, shuffle = @Key(group = {
            "word"}))) public class WordCountModel implements DataModel<WordCountModel>, Writable {
    private final StringOption word = new StringOption();
    private final LongOption count = new LongOption();
    @Override@SuppressWarnings("deprecation") public void reset() {
        this.word.setNull();
        this.count.setNull();
    }
    @Override@SuppressWarnings("deprecation") public void copyFrom(WordCountModel other) {
        this.word.copyFrom(other.word);
        this.count.copyFrom(other.count);
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
    /**
     * countを返す。
     * @return count
     * @throws NullPointerException countの値が<code>null</code>である場合
     */
    public long getCount() {
        return this.count.get();
    }
    /**
     * countを設定する。
     * @param value 設定する値
     */
    @SuppressWarnings("deprecation") public void setCount(long value) {
        this.count.modify(value);
    }
    /**
     * <code>null</code>を許すcountを返す。
     * @return count
     */
    public LongOption getCountOption() {
        return this.count;
    }
    /**
     * countを設定する。
     * @param option 設定する値、<code>null</code>の場合にはこのプロパティが<code>null</code>を表すようになる
     */
    @SuppressWarnings("deprecation") public void setCountOption(LongOption option) {
        this.count.copyFrom(option);
    }
    @Override public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("{");
        result.append("class=word_count_model");
        result.append(", word=");
        result.append(this.word);
        result.append(", count=");
        result.append(this.count);
        result.append("}");
        return result.toString();
    }
    @Override public int hashCode() {
        int prime = 31;
        int result = 1;
        result = prime * result + word.hashCode();
        result = prime * result + count.hashCode();
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
        WordCountModel other = (WordCountModel) obj;
        if(this.word.equals(other.word)== false) {
            return false;
        }
        if(this.count.equals(other.count)== false) {
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
        count.write(out);
    }
    @Override public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        count.readFields(in);
    }
}