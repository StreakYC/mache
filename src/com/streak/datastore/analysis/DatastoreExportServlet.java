package com.streak.datastore.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.google.appengine.tools.mapreduce.AppEngineJobContext;
import com.google.appengine.tools.mapreduce.ConfigurationXmlUtil;
import com.google.appengine.tools.mapreduce.DatastoreInputFormat;
import com.streak.logging.analysis.AnalysisConstants;
import com.streak.logging.analysis.AnalysisUtility;

public class DatastoreExportServlet extends HttpServlet {
	private String getBigqueryType(String datastoreStatsType) {
		datastoreStatsType = datastoreStatsType.toUpperCase();
		if (datastoreStatsType.equals("STRING") 
				|| datastoreStatsType.equals("TEXT") 
				|| datastoreStatsType.equals("REFERENCE")
				|| datastoreStatsType.equals("KEY")
				|| datastoreStatsType.equals("BLOBKEY")) {
			return "string";
		}
		if (datastoreStatsType.equals("INT64")
				|| datastoreStatsType.equals("INTEGER")
				|| datastoreStatsType.equals("DATE/TIME")) {
			return "integer";
		}
		if (datastoreStatsType.equals("BOOLEAN")) {
			return "boolean";
		}
		if (datastoreStatsType.equals("DOUBLE")
				|| datastoreStatsType.equals("FLOAT")) {
			return "float";
		}
		if (datastoreStatsType.equals("NULL") || datastoreStatsType.equals("BLOB")) {
			return null;
		}
		throw new RuntimeException("Couldn't map datastore stats type " + datastoreStatsType + " to BigQuery type");
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		String bucketName = req.getParameter(AnalysisConstants.BUCKET_NAME_PARAM);
		if (!AnalysisUtility.areParametersValid(bucketName)) {
			bucketName = getDefaultBucketName();
		}
		
		String exportName = req.getParameter(AnalysisConstants.EXPORT_NAME_PARAM);
		if (!AnalysisUtility.areParametersValid(exportName)) {
			exportName = getDefaultExportName();
		}
		
		String kindsToExcludeStr = req.getParameter(AnalysisConstants.KINDS_TO_EXCLUDE_PARAM);
		if (!AnalysisUtility.areParametersValid(kindsToExcludeStr)) {
			kindsToExcludeStr = "";
		}
		String shardCountStr = req.getParameter(AnalysisConstants.SHARD_COUNT_PARAM);
		int shardCount = getDefaultShardCount();
		if (AnalysisUtility.areParametersValid(shardCountStr)) {
			shardCount = 2;
		}
		String[] kindsToExcludeArray = kindsToExcludeStr.split(",");
		Set<String> kindsToExclude = new HashSet<String>(Arrays.asList(kindsToExcludeArray));
		
		String queueName = req.getParameter(AnalysisConstants.QUEUE_NAME_PARAM);
		if (!AnalysisUtility.areParametersValid(queueName)) {
			queueName = getDefaultQueueName();
		}
		
		DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
		PreparedQuery pq = datastore.prepare(new Query("__Stat_PropertyType_PropertyName_Kind__"));
		Map<String, Map<String, String>> kindNameTypeMap = new TreeMap<String, Map<String, String>>();
		for (Entity entity: pq.asIterable()) {
			String kindName = (String) entity.getProperty("kind_name");
			if (kindsToExclude.contains(kindName)) {
				continue;
			}
			String propertyName = (String) entity.getProperty("property_name");
			String propertyType = (String) entity.getProperty("property_type");
			Map<String, String> nameTypeMap = kindNameTypeMap.get(kindName);
			if (nameTypeMap == null) {
				nameTypeMap = new TreeMap<String, String>();
				kindNameTypeMap.put(kindName, nameTypeMap);
			}
			String bigqueryType = getBigqueryType(propertyType);
			if (bigqueryType == null) {
				continue;
			}
			String existingType = nameTypeMap.get(propertyName);
			if (existingType != null && !existingType.equals(bigqueryType)) {
				throw new RuntimeException(
						"Got two incompatible values for type of " + propertyName + ": " + existingType + " and " + bigqueryType);
			}
			nameTypeMap.put(propertyName, bigqueryType);
		}
		for (String kind : kindNameTypeMap.keySet()) {
			Map<String, String> entityMap = kindNameTypeMap.get(kind);
			List<String> names = new ArrayList<String>();
			List<String> types = new ArrayList<String>();
			for (String name : entityMap.keySet()) {
				names.add(name);
				types.add(entityMap.get(name));
			}
			String schemaKey = exportName + "_" + kind + ".schema";
			String outputKey = exportName + "_" + kind;
			AnalysisUtility.writeSchema(FileServiceFactory.getFileService(), bucketName, schemaKey, names, types);
			Configuration conf = new Configuration(false);
			conf.setClass("mapreduce.map.class", DatastoreBigqueryExportMapper.class, Mapper.class);
			conf.setClass("mapreduce.inputformat.class", DatastoreInputFormat.class, InputFormat.class);
			conf.set(AppEngineJobContext.CONTROLLER_QUEUE_KEY, queueName);
			conf.set(AppEngineJobContext.WORKER_QUEUE_KEY, queueName);
			conf.set(AppEngineJobContext.DONE_CALLBACK_QUEUE_KEY, queueName);
			conf.set(AppEngineJobContext.DONE_CALLBACK_URL_KEY, AnalysisUtility.getRequestBaseName(req) + 
					"/loadCloudStorageToBigquery?" + req.getQueryString() + "&" + AnalysisConstants.CLOUD_STORAGE_PATH_BASE_PARAM + "=" + outputKey);
			conf.set(DatastoreInputFormat.ENTITY_KIND_KEY, kind);
			conf.setInt(DatastoreInputFormat.SHARD_COUNT_KEY, shardCount);
			conf.set(DatastoreBigqueryExportMapper.OUTPUT_BUCKET, bucketName);
			conf.set(DatastoreBigqueryExportMapper.OUTPUT_KEY, outputKey);
			conf.set(DatastoreBigqueryExportMapper.SCHEMA_PATH, "/gs/" + bucketName + "/" + schemaKey);
			String xml = ConfigurationXmlUtil.convertConfigurationToXml(conf);

			Queue queue = QueueFactory.getQueue(queueName);
			TaskOptions task = TaskOptions.Builder.withDefaults()
					.url("/mapreduce/start")
					.method(Method.POST)
					.param("configuration", xml);
			queue.add(task);
		}
		
	}
	
	protected String getDefaultBucketName() {
		return "logs";
	}
	
	protected String getDefaultExportName() {
		return "dsExport";
	}
	
	protected int getDefaultShardCount() {
		return 2;
	}
	
	protected String getDefaultQueueName() {
		return QueueFactory.getDefaultQueue().getQueueName();
	}
}
