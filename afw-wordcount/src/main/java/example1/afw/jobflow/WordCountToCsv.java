package example1.afw.jobflow;

import example1.afw.modelgen.dmdl.csv.AbstractWordCountModelCsvOutputDescription;

public class WordCountToCsv extends AbstractWordCountModelCsvOutputDescription {

	@Override
	public String getBasePath() {
		return "example1/afwout";
	}

	@Override
	public String getResourcePattern() {
		return "wordcount*.txt";
	}
}
