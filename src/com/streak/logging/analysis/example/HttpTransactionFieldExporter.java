package com.streak.logging.analysis.example;

import java.util.Arrays;
import java.util.List;

import com.google.appengine.api.log.RequestLogs;
import com.streak.logging.analysis.BigqueryFieldExporter;

public class HttpTransactionFieldExporter implements BigqueryFieldExporter {
	private static final List<String> NAMES = Arrays.asList(
			"httpStatus", "method", "httpVersion");
	
	private int httpStatus;
	private String method;
	private String httpVersion;
	
	@Override
	public void processLog(RequestLogs log) {
		httpStatus = log.getStatus();
		method = log.getMethod();
		httpVersion = log.getHttpVersion();
	}

	@Override
	public Object getField(String name) {
		if (name == "httpStatus") {
			return httpStatus;
		}
		if (name == "method") {
			return method;
		}
		if (name == "httpVersion") {
			return httpVersion;
		}
		
		return null;
	}

	@Override
	public int getFieldCount() {
		return NAMES.size();
	}

	@Override
	public String getFieldName(int i) {
		return NAMES.get(i);
	}

	@Override
	public String getFieldType(int i) {
		if (i == 0) {
			return "integer";
		}
		return "string";
	}

}
