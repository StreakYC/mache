package com.streak.logging.analysis.example;

import java.util.Arrays;
import java.util.List;

import com.google.appengine.api.log.RequestLogs;
import com.streak.logging.analysis.BigqueryFieldExporter;

public class VersionFieldExporter implements BigqueryFieldExporter {
	private String versionId;
	
	@Override
	public void processLog(RequestLogs log) {
		versionId = log.getVersionId();
	}

	@Override
	public Object getField(String name) {
		return versionId;
	}

	@Override
	public int getFieldCount() {
		return 1;
	}

	@Override
	public String getFieldName(int i) {
		return "versionId";
	}

	@Override
	public String getFieldType(int i) {
		return "string";
	}

}
