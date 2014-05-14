/*
 * Copyright 2012 Rewardly Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streak.logging.utils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.json.JSONException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.api.utils.SystemProperty.Environment.Value;
import com.streak.datastore.analysis.builtin.BuiltinDatastoreExportConfiguration;
import com.streak.logging.analysis.LogsExportConfiguration;
import com.streak.logging.analysis.LogsFieldExporter;
import com.streak.logging.analysis.LogsFieldExporterSet;

public class AnalysisUtility {

	public static boolean isDev() {
		return SystemProperty.environment.value() == Value.Development;
	}
	
	public static long round(long x, long roundMultiple) {
		return (x / roundMultiple) * roundMultiple;
	}
	
	public static boolean areParametersValid(String... params) {
		if (params == null) {
			return false;
		}
		
		for (String p : params) {
			if (p == null || p.isEmpty()) {
				return false;
			}
		}
		return true;
	}

	public static String extractParameterOrThrow(HttpServletRequest req, String paramName) {
		String param = req.getParameter(paramName);
		if (!areParametersValid(param)) {
			throw new InvalidTaskParameterException("Couldn't find required parameter " + paramName);
		}
		return param;
	}

	public static String escapeAndQuoteFieldCsv(String field) {
		return "\"" + field.replace("\"", "\"\"").replace("\r\n", "\r").replace("\n", "\r") + "\"";
	}

	public static String createLogTableKey(String schemaHash, long startTime, long endTime) {
		return String.format("log_%s_%020d_%020d", schemaHash, startTime, endTime);
	}

	public static String createSchemaKey(String schemaHash, long startTime, long endTime) {
		return createLogTableKey(schemaHash, startTime, endTime) + ".schema";
	}

	public static long getEndMsFromKey(String key) {
		String[] keyParts = key.split("_");
		return Long.parseLong(keyParts[keyParts.length - 1]);
	}

	
	public static void fetchCloudStorageLogUris(
			String bucketName,
			String schemaHash,
			long startMs,
			long endMs,
			HttpRequestFactory requestFactory,
			List<String> urisToProcess,
			boolean readSchemas) throws IOException {
		String startKey = AnalysisUtility.createLogTableKey(schemaHash, startMs, startMs);
		String endKey = AnalysisUtility.createLogTableKey(schemaHash, endMs, endMs);
		fetchCloudStorageUris(bucketName, startKey, endKey, requestFactory,
				urisToProcess, readSchemas);
	}

	public static void fetchCloudStorageUris(String bucketName,
			String startKey, String endKey, HttpRequestFactory requestFactory,
			List<String> urisToProcess, boolean readSchemas) throws IOException {
		String bucketUri = "http://commondatastorage.googleapis.com/" + bucketName;
		HttpRequest request = requestFactory.buildGetRequest(
				new GenericUrl(bucketUri + "?marker=" + startKey));
		HttpResponse response = request.execute();

		try {
			Document responseDoc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(response.getContent());
			XPath xPath = XPathFactory.newInstance().newXPath();
			NodeList nodes = (NodeList) xPath.evaluate("//Contents/Key/text()", responseDoc, XPathConstants.NODESET);
			for (int i = 0; i < nodes.getLength(); i++) {
				String key = nodes.item(i).getNodeValue();
				if (key.compareTo(endKey) >= 0) {
					break;
				}
				if (key.endsWith(".schema") ^ readSchemas) {
					continue;
				}
				if (readSchemas) {
					key = key.substring(0, key.length() - ".schema".length());
				}
				urisToProcess.add("gs://" + bucketName + "/" + key);
			} 
		} catch (SAXException e) {
			throw new IOException("Error parsing cloud storage response", e);
		} catch (ParserConfigurationException e) {
			throw new IOException("Error configuring cloud storage parser", e);
		} catch (XPathExpressionException e) {
			throw new IOException("Error finding keys", e);
		}
	}

	public static String getRequestBaseName(HttpServletRequest req) {
		String path = req.getRequestURI();
		return path.substring(0, path.lastIndexOf("/"));
	}

	public static LogsExportConfiguration instantiateLogExporterConfig(String logsExportConfigurationClassStr) {
		Class<?> exporterConfigClass;
		try {
			exporterConfigClass = Class.forName(logsExportConfigurationClassStr);
		}
		catch (ClassNotFoundException e) {
			throw new InvalidTaskParameterException("Got invalid BigqueryFieldExporterSet class name: " + logsExportConfigurationClassStr);
		}
		
		if (!LogsExportConfiguration.class.isAssignableFrom(exporterConfigClass)) {
			throw new InvalidTaskParameterException("Got logsExportConfiguration parameter " 
					+ logsExportConfigurationClassStr + " that doesn't implement: " + LogsExportConfiguration.class.getSimpleName());
		}
		
		LogsExportConfiguration exporterConfig;
		try {
			exporterConfig = (LogsExportConfiguration) exporterConfigClass.newInstance();
		}
		catch (InstantiationException e) {
			throw new InvalidTaskParameterException("Couldn't instantiate BigqueryFieldExporter set class " + logsExportConfigurationClassStr);
		}
		catch (IllegalAccessException e) {
			throw new InvalidTaskParameterException("LogsExportConfiguration class " + logsExportConfigurationClassStr + " has no visible default constructor");
		}
		return exporterConfig;
	}

	public static String computeSchemaHash(LogsFieldExporterSet exporterSet) {
		try {
			List<LogsFieldExporter> exporters = exporterSet.getExporters();
			MessageDigest md = MessageDigest.getInstance("MD5");
			for (LogsFieldExporter exporter : exporters) {
				for (int i = 0; i < exporter.getFieldCount(); i++) {
					md.update(exporter.getFieldName(i).getBytes("UTF-8"));
					md.update(exporter.getFieldType(i).getBytes("UTF-8"));
				}
			}
			byte[] array = md.digest();
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < array.length; ++i) {
				sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1,3));
			}
			return sb.toString().substring(0, 6);
		} catch (NoSuchAlgorithmException nsae) {
			throw new RuntimeException("Couldn't find MD5 algorithm for schema hash", nsae);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Couldn't get UTF-8 encoding for schema hash", e);
		}
	}
	
	public static TableSchema createSchema(LogsFieldExporterSet exporterSet) {
		Set<String> fieldNames = new HashSet<String>();
		
		List<LogsFieldExporter> exporters = exporterSet.getExporters();
		TableSchema schema = new TableSchema();
		schema.setFields(new ArrayList<TableFieldSchema>());
		
		for (LogsFieldExporter exporter : exporters) {
			for (int i = 0; i < exporter.getFieldCount(); i++) {
				String fieldName = exporter.getFieldName(i);
				String fieldMode = "NULLABLE";
				String fieldType = exporter.getFieldType(i).toLowerCase().intern();
				
				if (fieldNames.contains(fieldName)) {
					throw new InvalidFieldException("BigqueryFieldExporterSet " + exporterSet.getClass().getCanonicalName()
							+ " defines multiple fields with name " + exporter.getFieldName(i));
				}
				fieldNames.add(fieldName);
				
				if (exporter.getFieldRepeated(i)) {
					fieldMode = "REPEATED";
				}
				
				List<TableFieldSchema> subfields = null;
				if (fieldType.equals("record")) {
					subfields = exporter.getFieldFields(i);
				}
				
				TableFieldSchema tfs = new TableFieldSchema();
				tfs.setName(fieldName);
				tfs.setType(fieldType);
				tfs.setMode(fieldMode);
				
				if (subfields != null) {
					tfs.setFields(subfields);
				}
				schema.getFields().add(tfs);
			}
		}
		return schema;
	}
	
	public static void putJsonValueFormatted(Map<String, Object> row, String fieldName, Object fieldValue, String fieldType) throws JSONException {
		// These strings have been interned so == works for comparison
		if ("string" == fieldType) {
			String stringValue;
			if (fieldValue instanceof Text) {
				stringValue = ((Text) fieldValue).getValue();
			}
			else {
				stringValue = fieldValue.toString();
			}
			row.put(fieldName, stringValue);
		}
		else if ("float" == fieldType) {
			row.put(fieldName, fieldValue);
		}
		else if ("integer" == fieldType) {
			if (fieldValue instanceof Date) {
				@SuppressWarnings("unused")
				long dateLong = ((Date) fieldValue).getTime();
				row.put(fieldName, fieldValue);
			}
			else {
				row.put(fieldName, fieldValue);
			}
		}
		else {
			row.put(fieldName, fieldValue);
		}
	}

	public static String failureJson(String message) {
		return "{\"success\":false, \"message\":\"" + message + "\"}";
	}
	
	public static String successJson(String message) {
		return "{\"success\":true, \"message\":\"" + message + "\"}";
	}
	
	public static BuiltinDatastoreExportConfiguration instantiateDatastoreExportConfig(String builtinDatastoreExportConfig) {
		Class<?> exportConfigClass;
		try {
			exportConfigClass = Class.forName(builtinDatastoreExportConfig);
		}
		catch (ClassNotFoundException e) {
			throw new InvalidTaskParameterException("Got invalid BuiltinDatastoreExportConfig class name: " + builtinDatastoreExportConfig);
		}
		if (!BuiltinDatastoreExportConfiguration.class.isAssignableFrom(exportConfigClass)) {
			throw new InvalidTaskParameterException("Got bigqueryFieldExporterSet parameter " + builtinDatastoreExportConfig
					+ " that doesn't implement BigqueryFieldExporterSet");
		}
		BuiltinDatastoreExportConfiguration exportConfig;
		try {
			exportConfig = (BuiltinDatastoreExportConfiguration) exportConfigClass.newInstance();
		}
		catch (InstantiationException e) {
			throw new InvalidTaskParameterException("Couldn't instantiate BigqueryFieldExporter set class " + builtinDatastoreExportConfig);
		}
		catch (IllegalAccessException e) {
			throw new InvalidTaskParameterException("BigqueryFieldExporter class " + builtinDatastoreExportConfig + " has no visible default constructor");
		}
		return exportConfig;
	}

	public static String getPreBackupName(long timestamp, String backupNamePrefix) {
		return backupNamePrefix + timestamp + "_";
	}
}
