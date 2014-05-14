package com.streak.logging.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;

public class BigqueryIngester {
	private static final Logger log = Logger.getLogger("bqlogging");

	public static TableDataInsertAllResponse streamingRowIngestion(Map<String, Object> row, String tableId, String datasetId, String projectId, Bigquery bigquery) throws IOException {
		return streamingRowIngestion(row, null, tableId, datasetId, projectId, bigquery);
	}
	
	public static TableDataInsertAllResponse streamingRowIngestion(Map<String, Object> row, String insertId, String tableId, String datasetId, String projectId, Bigquery bigquery) throws IOException {
		return streamingRowIngestion(Arrays.asList(row), Arrays.asList(insertId), tableId, datasetId, projectId, bigquery);
	}

	public static TableDataInsertAllResponse streamingRowIngestion(List<Map<String, Object>> rows, String tableId, String datasetId, String projectId, Bigquery bigquery) throws IOException {
		return streamingRowIngestion(rows, null, tableId, datasetId, projectId, bigquery);
	}

	public static TableDataInsertAllResponse streamingRowIngestion(List<Map<String, Object>> rows, List<String> insertIds, String tableId, String datasetId, String projectId, Bigquery bigquery) throws IOException {
		assert insertIds == null || rows.size() == insertIds.size(); 
	
		if (rows.size() == 0) {
			return null;
		}
		
		log.warning("streamingRowIngestion Number of Rows: " + rows.size());
		
		ArrayList<TableDataInsertAllRequest.Rows> rowList = new ArrayList<>();
		for (int i = 0; i < rows.size(); i++) {
			Map<String, Object> row = rows.get(i);
			String insertId = null;
			if (insertIds != null) {
				insertId = insertIds.get(i);
			}
			
			TableDataInsertAllRequest.Rows requestRow = new TableDataInsertAllRequest.Rows();
			requestRow.setJson(row);
			requestRow.setInsertId(insertId);
			rowList.add(requestRow);
		}

		TableDataInsertAllRequest content = new TableDataInsertAllRequest().setRows(rowList);
		TableDataInsertAllResponse response = bigquery.tabledata().insertAll(projectId, datasetId, tableId, content).execute();
		 
		if (response.getInsertErrors() != null && response.getInsertErrors().size() > 0) {
			logInsertErrors(response.getInsertErrors(), rows);
		}
		
		return response;
	}
	
	private static void logInsertErrors(List<InsertErrors> insertErrors, List<Map<String, Object>> rows) {
		log.warning(insertErrors.size() + " insert errors");
		
		Map<String, Integer> reasonCounts = new HashMap<String, Integer>();
		int i = 0;
		for (InsertErrors errorSet : insertErrors) {
			for (ErrorProto singleError : errorSet.getErrors()) {
				String reason = singleError.getReason() + "~" + singleError.getMessage();
				if (reasonCounts.get(reason) == null) {
					reasonCounts.put(reason, 0);
				}
				reasonCounts.put(reason, reasonCounts.get(reason) + 1);
				
				if ("Maximum allowed row size exceeded".equals(singleError.getMessage())) {
					Map<String, Object> row = rows.get(i);
					for (String k : row.keySet()) {
						log.warning(k + ": " + (row.get(k) == null ? 0 : row.get(k).toString().length()));
					}
					log.warning("Path: " + row.get("path"));
				}
			}
			i++;
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append("InsertErrors --------------");
		for (String k : reasonCounts.keySet()) {
			sb.append(k).append(",").append(reasonCounts.get(k)).append("\n");
		}
		log.warning(sb.toString());
	}

	public static Table createTable(String projectId, String datasetId, String tableId, TableSchema schema, Bigquery bigquery) throws IOException {
		Table table = new Table();

		TableReference tableRef = new TableReference();
		tableRef.setDatasetId(datasetId);
		tableRef.setProjectId(projectId);
		tableRef.setTableId(tableId);
		table.setTableReference(tableRef);
		table.setFriendlyName(tableId);
		table.setSchema(schema);

		try {
			return bigquery.tables().insert(projectId, datasetId, table).execute();
		}
		catch (GoogleJsonResponseException e) {
			return null; //table already exists
		}
	}
}
