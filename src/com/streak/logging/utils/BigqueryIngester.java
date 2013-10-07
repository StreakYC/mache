package com.streak.logging.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;

public class BigqueryIngester {
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
		TableDataInsertAllResponse response = null;
		
		response = bigquery.tabledata().insertAll(projectId, datasetId, tableId, content).execute();
		System.out.println(response.toPrettyString());
		
		return response;
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
