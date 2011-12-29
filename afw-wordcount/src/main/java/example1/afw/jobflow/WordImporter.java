package example1.afw.jobflow;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.asakusafw.vocabulary.external.FileImporterDescription;

import example1.afw.modelgen.dmdl.model.WordModel;

public class WordImporter extends FileImporterDescription {

	@Override
	public Class<?> getModelType() {
		return WordModel.class;
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
		return WordModelInputFormat.class;
	}
}
