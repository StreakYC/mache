package com.streak.logging.analysis.example;

import java.util.Arrays;
import java.util.List;

import com.google.appengine.api.log.RequestLogs;
import com.streak.logging.analysis.BigqueryFieldExporter;

public class PerformanceFieldExporter implements BigqueryFieldExporter {
	private static final List<String> NAMES = Arrays.asList(
			"apiMcycles", 
			"cost", 
			"responseSize", 
			"mcycles",
			"loadingRequest", 
			"pendingTimeUsec",
			"latencyUsec");
	
	private static final List<String> TYPES = Arrays.asList(
			"integer", // apiMcycles
			"float",   // cost
			"integer", // responseSize
			"integer", // mcycles
			"boolean", // loadingRequest
			"integer", // pendingTimeUsec
			"integer"  // latencyUsec
			);
	
	private long apiMcycles, responseSize, mcycles, pendingTimeUsec, latencyUsec;
	private boolean loadingRequest;
	private double cost;
	
	@Override
	public void processLog(RequestLogs log) {
		apiMcycles = log.getApiMcycles();
		cost = log.getCost();
		responseSize = log.getResponseSize();
		mcycles = log.getMcycles();
		loadingRequest = log.isLoadingRequest();
		pendingTimeUsec = log.getPendingTimeUsec();
		latencyUsec = log.getLatencyUsec();
	}

	@Override
	public Object getField(String name) {
		if (name == "apiMcycles") {
			return apiMcycles;
		}
		if (name == "cost") {
			return cost;
		}
		if (name == "responseSize") {
			return responseSize;
		}
		if (name == "mcycles") {
			return mcycles;
		}
		if (name == "loadingRequest") {
			return loadingRequest;
		}
		if (name == "pendingTimeUsec") {
			return pendingTimeUsec;
		}
		if (name == "latencyUsec") {
			return latencyUsec;
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
		return TYPES.get(i);
	}

}
