package com.streak.logging.analysis.example;

import com.google.appengine.api.log.RequestLogs;
import com.streak.logging.analysis.BigqueryFieldExporter;

public class InstanceFieldExporter implements BigqueryFieldExporter {
	private String instanceKey;
	
	@Override
	public void processLog(RequestLogs log) {
		instanceKey = log.getInstanceKey();
	}

	@Override
	public Object getField(String name) {
		return instanceKey;
	}

	@Override
	public int getFieldCount() {
		return 1;
	}

	@Override
	public String getFieldName(int i) {
		return "instanceKey";
	}

	@Override
	public String getFieldType(int i) {
		return "string";
	}
}
