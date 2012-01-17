package example1.afw.operator;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.flow.Operator;
import com.asakusafw.vocabulary.flow.Source;
import com.asakusafw.vocabulary.flow.graph.FlowBoundary;
import com.asakusafw.vocabulary.flow.graph.FlowElementResolver;
import com.asakusafw.vocabulary.flow.graph.ObservationCount;
import com.asakusafw.vocabulary.flow.graph.OperatorDescription;
import com.asakusafw.vocabulary.flow.graph.ShuffleKey;
import com.asakusafw.vocabulary.flow.processor.PartialAggregation;
import com.asakusafw.vocabulary.operator.Extract;
import com.asakusafw.vocabulary.operator.Summarize;
import example1.afw.modelgen.dmdl.model.TextModel;
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
    public static final class Split implements Operator {
        private final FlowElementResolver $;
        /**
         */
        public final Source<WordModel> out;
        Split(Source<TextModel> tm) {
            OperatorDescription.Builder builder = new OperatorDescription.Builder(Extract.class);
            builder.declare(WordCountOperator.class, WordCountOperatorImpl.class, "split");
            builder.declareParameter(TextModel.class);
            builder.declareParameter(Result.class);
            builder.addInput("tm", tm);
            builder.addOutput("out", WordModel.class);
            builder.addAttribute(ObservationCount.DONT_CARE);
            this.$ = builder.toResolver();
            this.$.resolveInput("tm", tm);
            this.out = this.$.resolveOutput("out");
        }
        /**
         * この演算子の名前を設定する。
         * @param newName 設定する名前
         * @return この演算子オブジェクト (this)
         * @throws IllegalArgumentException 引数に{@code null}が指定された場合
         */
        public WordCountOperatorFactory.Split as(String newName) {
            this.$.setName(newName);
            return this;
        }
    }
    /**
     * @param tm
     * @return 生成した演算子オブジェクト
     * @see WordCountOperator#split(TextModel, Result)
     */
    public WordCountOperatorFactory.Split split(Source<TextModel> tm) {
        return new WordCountOperatorFactory.Split(tm);
    }
    /**
     */
    public static final class Count implements Operator {
        private final FlowElementResolver $;
        /**
         */
        public final Source<WordCountModel> out;
        Count(Source<WordModel> wm) {
            OperatorDescription.Builder builder0 = new OperatorDescription.Builder(Summarize.class);
            builder0.declare(WordCountOperator.class, WordCountOperatorImpl.class, "count");
            builder0.declareParameter(WordModel.class);
            builder0.addInput("wm", wm, new ShuffleKey(Arrays.asList(new String[]{"word"}), Arrays.asList(new ShuffleKey
                    .Order[]{})));
            builder0.addOutput("out", WordCountModel.class);
            builder0.addAttribute(FlowBoundary.SHUFFLE);
            builder0.addAttribute(ObservationCount.DONT_CARE);
            builder0.addAttribute(PartialAggregation.PARTIAL);
            this.$ = builder0.toResolver();
            this.$.resolveInput("wm", wm);
            this.out = this.$.resolveOutput("out");
        }
        /**
         * この演算子の名前を設定する。
         * @param newName0 設定する名前
         * @return この演算子オブジェクト (this)
         * @throws IllegalArgumentException 引数に{@code null}が指定された場合
         */
        public WordCountOperatorFactory.Count as(String newName0) {
            this.$.setName(newName0);
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