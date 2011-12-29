package example1.afw.operator;
import com.asakusafw.vocabulary.flow.Operator;
import com.asakusafw.vocabulary.flow.Source;
import com.asakusafw.vocabulary.flow.graph.FlowBoundary;
import com.asakusafw.vocabulary.flow.graph.FlowElementResolver;
import com.asakusafw.vocabulary.flow.graph.ObservationCount;
import com.asakusafw.vocabulary.flow.graph.OperatorDescription;
import com.asakusafw.vocabulary.flow.graph.ShuffleKey;
import com.asakusafw.vocabulary.flow.processor.PartialAggregation;
import com.asakusafw.vocabulary.operator.Summarize;
import example1.afw.modelgen.dmdl.model.WordCountModel;
import example1.afw.modelgen.dmdl.model.WordModel;
import java.util.Arrays;
import javax.annotation.Generated;
/**
 * {@link WordCountOperator}に関する演算子ファクトリークラス。
 * @see WordCountOperator
 */
@Generated("OperatorFactoryClassGenerator:0.0.1") public class WordCountOperatorFactory {
    /**
     */
    public static final class Count implements Operator {
        private final FlowElementResolver $;
        /**
         */
        public final Source<WordCountModel> out;
        Count(Source<WordModel> wm) {
            OperatorDescription.Builder builder = new OperatorDescription.Builder(Summarize.class);
            builder.declare(WordCountOperator.class, WordCountOperatorImpl.class, "count");
            builder.declareParameter(WordModel.class);
            builder.addInput("wm", wm, new ShuffleKey(Arrays.asList(new String[]{"word"}), Arrays.asList(new ShuffleKey.
                    Order[]{})));
            builder.addOutput("out", WordCountModel.class);
            builder.addAttribute(FlowBoundary.SHUFFLE);
            builder.addAttribute(ObservationCount.DONT_CARE);
            builder.addAttribute(PartialAggregation.DEFAULT);
            this.$ = builder.toResolver();
            this.$.resolveInput("wm", wm);
            this.out = this.$.resolveOutput("out");
        }
        /**
         * この演算子の名前を設定する。
         * @param newName 設定する名前
         * @return この演算子オブジェクト (this)
         * @throws IllegalArgumentException 引数に{@code null}が指定された場合
         */
        public WordCountOperatorFactory.Count as(String newName) {
            this.$.setName(newName);
            return this;
        }
    }
    /**
     * @param wm
     * @return 生成した演算子オブジェクト
     * @see WordCountOperator#count(WordModel)
     */
    public WordCountOperatorFactory.Count count(Source<WordModel> wm) {
        return new WordCountOperatorFactory.Count(wm);
    }
}