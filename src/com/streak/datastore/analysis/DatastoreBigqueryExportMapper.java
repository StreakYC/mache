package com.streak.datastore.analysis;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.mapreduce.AppEngineMapper;
import com.streak.logging.analysis.AnalysisUtility;
import com.streak.logging.analysis.FancyFileWriter;

public class DatastoreBigqueryExportMapper extends
		AppEngineMapper<Key, Entity, NullWritable, NullWritable> {
	public static final String WRITABLE_MAPPING = "_MACHE_WritableMapping_";
	public static final String OUTPUT_KEY = "cloudstorage.output.path";
	public static final String OUTPUT_BUCKET = "cloudstorage.output.bucket";
	public static final String SCHEMA_PATH = "cloudstorage.schema.path";
	
	private static final Logger log = Logger.getLogger(FancyFileWriter.class.getName());

	private String[] names;
	private String[] types;
	private FancyFileWriter writer;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		openWriter(context, true);
		String fullPath = writer.getWritablePath();
		Entity writableEntity = new Entity(WRITABLE_MAPPING, getTaskKey(context));
		writableEntity.setProperty("writable", new Text(fullPath));
		DatastoreServiceFactory.getDatastoreService().put(writableEntity);
		writer.closeTemporarily();
	}
	
	@Override
	public void taskSetup(Context context) throws IOException,
			InterruptedException {
		super.taskSetup(context);
		String schema = AnalysisUtility.loadSchemaStr(context.getConfiguration().get(SCHEMA_PATH));
		String[] schemaParts = schema.split(",");
		names = new String[schemaParts.length];
		types = new String[schemaParts.length];
		for (int i = 0; i < schemaParts.length; i++) {
			String[] schemaFieldParts = schemaParts[i].split(":");
			names[i] = schemaFieldParts[0].intern();
			types[i] = schemaFieldParts[1].intern();
		}
		openWriter(context, false);
	}

	private void openWriter(Context context, boolean create) throws IOException {
		if (create) {
			writer = new FancyFileWriter(
					context.getConfiguration().get(OUTPUT_BUCKET), 
					getTaskKey(context));
		} else {
			try {
				Entity writableEntity = 
						DatastoreServiceFactory.getDatastoreService().get(
								KeyFactory.createKey(WRITABLE_MAPPING, getTaskKey(context)));

				String fullPath = ((Text) writableEntity.getProperty("writable")).getValue();
				log.warning("Got writable path: " + fullPath);
				writer = new FancyFileWriter(fullPath);
			} catch (EntityNotFoundException e) {
				throw new IOException(e);
			}
		}
	}

	private String getTaskKey(Context context) {
		return context.getConfiguration().get(OUTPUT_KEY) 
			+ "_" + context.getTaskAttemptID().getTaskID().getId();
	}
	
	@Override
	public void taskCleanup(Context context) throws IOException,
			InterruptedException {
		super.taskCleanup(context);
		writer.closeTemporarily();
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		openWriter(context, false);
		writer.closeFinally();
		DatastoreServiceFactory.getDatastoreService().delete(
				KeyFactory.createKey(WRITABLE_MAPPING, getTaskKey(context)));
	}

	@Override
	public void map(Key key, Entity entity, Mapper<Key,Entity,NullWritable, NullWritable>.Context context) throws IOException, InterruptedException {
		for (int i = 0; i < names.length; i++) {
			writer.append(AnalysisUtility.formatCsvValue(entity.getProperty(names[i]), types[i]));
			if (i < names.length - 1) {
				writer.append(",");
			}
		}
		writer.append("\n");
	}
}
