package com.streak.logging.analysis;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.util.logging.Logger;

import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileWriteChannel;
import com.google.appengine.api.files.FinalizationException;
import com.google.appengine.api.files.GSFileOptions.GSFileOptionsBuilder;
import com.google.appengine.api.files.LockException;

/**
 * FancyFileWriter sands down the rough edges of App Engine file API.
 * It automatically handles the gymnastics of opening the file,
 * reopening it periodically, and buffer output.
 */
public class FancyFileWriter {
	// Batch writes so that they're at least FILE_BUFFER_LIMIT bytes
	private static final int FILE_BUFFER_LIMIT = 100000;

	// Reopen file every OPEN_MILLIS_LIMIT ms
	private static final int OPEN_MILLIS_LIMIT = 20000;
	
	private static final Logger log = Logger.getLogger(FancyFileWriter.class.getName());

	private static FileService fileService = FileServiceFactory.getFileService();
	
	private long lastOpenMillis;
	private AppEngineFile logsFile;
	private FileWriteChannel logsChannel;
	private PrintWriter logsWriter;
	private StringBuffer sb = new StringBuffer();
	private boolean closed = false;
	
	public FancyFileWriter(String bucketName, String fileKey) throws IOException {
		lastOpenMillis = System.currentTimeMillis();
		
		log.warning("Creating file " + bucketName + ":" + fileKey);
		GSFileOptionsBuilder optionsBuilder = new GSFileOptionsBuilder()
			.setBucket(bucketName)
			.setKey(fileKey)
			.setAcl("project-private");
		logsFile = fileService.createNewGSFile(optionsBuilder.build());
		init();
	}
	
	public FancyFileWriter(String writablePath) throws IOException {
		log.warning("Reopening file "+ writablePath);
		logsFile = new AppEngineFile(writablePath);
		init();
	}
	
	private void init() throws FileNotFoundException, FinalizationException,
			LockException, IOException {
		logsChannel = fileService.openWriteChannel(logsFile, true);
		logsWriter = new PrintWriter(Channels.newWriter(logsChannel, "UTF8"));
	}
	
	public void append(String s) throws IOException {
		if (closed) {
			throw new IOException("Tried to append to closed FancyFileWriter");
		}
		
		sb.append(s);
		
		if (System.currentTimeMillis() - lastOpenMillis > OPEN_MILLIS_LIMIT) {
			logsWriter.close();
			init();
			lastOpenMillis = System.currentTimeMillis();
		}
		if (sb.length() > FILE_BUFFER_LIMIT) {
			logsWriter.print(sb);
			sb.delete(0,  sb.length());
		}
	}
	
	public void closeTemporarily() throws IOException {
		log.warning("Closing file " + logsFile.getFullPath() + " temporarily " + closed);
		if (closed) {
			return;
		}
		closed = true;
		if (sb.length() > 0) {
			logsWriter.print(sb);
			sb.delete(0,  sb.length());
		}

		logsWriter.close();
	}
	
	public void closeFinally() throws IOException {
		log.warning("Closing file " + logsFile.getFullPath() + " finally " + closed);
		if (closed) {
			return;
		}
		
		closeTemporarily();
		logsChannel.closeFinally();
	}
	
	public String getWritablePath() throws IOException {
		return logsFile.getFullPath();
	}
}
