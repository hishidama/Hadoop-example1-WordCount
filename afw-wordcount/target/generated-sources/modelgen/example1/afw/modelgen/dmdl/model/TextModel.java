package example1.afw.modelgen.dmdl.model;
import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.model.DataModelKind;
import com.asakusafw.runtime.model.ModelInputLocation;
import com.asakusafw.runtime.model.ModelOutputLocation;
import com.asakusafw.runtime.model.PropertyOrder;
import com.asakusafw.runtime.value.StringOption;
import com.asakusafw.vocabulary.bulkloader.ColumnOrder;
import com.asakusafw.vocabulary.bulkloader.OriginalName;
import example1.afw.modelgen.dmdl.io.TextModelInput;
import example1.afw.modelgen.dmdl.io.TextModelOutput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/**
 * text_modelを表すデータモデルクラス。
 */
@ColumnOrder(value = {"TEXT"})@DataModelKind("DMDL")@ModelInputLocation(TextModelInput.class)@ModelOutputLocation(
        TextModelOutput.class)@OriginalName(value = "TEXT_MODEL")@PropertyOrder({"text"}) public class TextModel 
        implements DataModel<TextModel>, Writable {
    private final StringOption text = new StringOption();
    @Override@SuppressWarnings("deprecation") public void reset() {
        this.text.setNull();
    }
    @Override@SuppressWarnings("deprecation") public void copyFrom(TextModel other) {
        this.text.copyFrom(other.text);
    }
    /**
     * textを返す。
     * @return text
     * @throws NullPointerException textの値が<code>null</code>である場合
     */
    public Text getText() {
        return this.text.get();
    }
    /**
     * textを設定する。
     * @param value 設定する値
     */
    @SuppressWarnings("deprecation") public void setText(Text value) {
        this.text.modify(value);
    }
    /**
     * <code>null</code>を許すtextを返す。
     * @return text
     */
    public StringOption getTextOption() {
        return this.text;
    }
    /**
     * textを設定する。
     * @param option 設定する値、<code>null</code>の場合にはこのプロパティが<code>null</code>を表すようになる
     */
    @SuppressWarnings("deprecation") public void setTextOption(StringOption option) {
        this.text.copyFrom(option);
    }
    @Override public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("{");
        result.append("class=text_model");
        result.append(", text=");
        result.append(this.text);
        result.append("}");
        return result.toString();
    }
    @Override public int hashCode() {
        int prime = 31;
        int result = 1;
        result = prime * result + text.hashCode();
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
        TextModel other = (TextModel) obj;
        if(this.text.equals(other.text)== false) {
            return false;
        }
        return true;
    }
    /**
     * textを返す。
     * @return text
     * @throws NullPointerException textの値が<code>null</code>である場合
     */
    public String getTextAsString() {
        return this.text.getAsString();
    }
    /**
     * textを設定する。
     * @param text0 設定する値
     */
    @SuppressWarnings("deprecation") public void setTextAsString(String text0) {
        this.text.modify(text0);
    }
    @Override public void write(DataOutput out) throws IOException {
        text.write(out);
    }
    @Override public void readFields(DataInput in) throws IOException {
        text.readFields(in);
    }
}