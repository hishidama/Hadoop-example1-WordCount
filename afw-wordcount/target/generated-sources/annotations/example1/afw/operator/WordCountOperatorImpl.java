package example1.afw.operator;
import example1.afw.modelgen.dmdl.model.WordCountModel;
import example1.afw.modelgen.dmdl.model.WordModel;
import javax.annotation.Generated;
/**
 * {@link WordCountOperator}に関する演算子実装クラス。
 */
@Generated("OperatorImplementationClassGenerator:0.0.1") public class WordCountOperatorImpl extends WordCountOperator {
    /**
     * インスタンスを生成する。
     */
    public WordCountOperatorImpl() {
        return;
    }
    @Override public WordCountModel count(WordModel wm) {
        throw new UnsupportedOperationException("単純集計演算子は組み込みの方法で処理されます");
    }
}