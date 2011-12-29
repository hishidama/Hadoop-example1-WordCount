package example1.afw.jobflow;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.asakusafw.vocabulary.external.FileExporterDescription;

import example1.afw.modelgen.dmdl.model.WordCountModel;

public class WordCountExporter extends FileExporterDescription {

	@Override
	public Class<?> getModelType() {
		return WordCountModel.class;
	}

	@Override
	public String getPathPrefix() {
		return "example1/afwout/result-*";
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends FileOutputFormat> getOutputFormat() {
		return WordCountModelOutputFormat.class;
	}
}
