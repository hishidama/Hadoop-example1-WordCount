package example1.afw.jobflow;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.asakusafw.vocabulary.external.FileImporterDescription;

import example1.afw.modelgen.dmdl.model.TextModel;

public class TextImporter extends FileImporterDescription {

	@Override
	public Class<?> getModelType() {
		return TextModel.class;
	}

	@Override
	public Set<String> getPaths() {
		Set<String> set = new HashSet<String>();
		set.add("example1/input/*");
		return set;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends FileInputFormat> getInputFormat() {
		return TextModelInputFormat.class;
	}
}
