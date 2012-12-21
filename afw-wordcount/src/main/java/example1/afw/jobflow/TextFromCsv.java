package example1.afw.jobflow;

import example1.afw.modelgen.dmdl.csv.AbstractTextModelCsvInputDescription;

public class TextFromCsv extends AbstractTextModelCsvInputDescription {

	@Override
	public String getBasePath() {
		return "example1/input";
	}

	@Override
	public String getResourcePattern() {
		return "*.txt";
	}

	@Override
	public DataSize getDataSize() {
		return DataSize.LARGE;
	}
}
