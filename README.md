# What is this?

The Mache framework is a Java App Engine library that makes it easy
to export your App Engine application logs to BigQuery for analysis. It
consists of a cron task that periodically copies data from the App Engine
LogService to a BigQuery table, and lets you customize the parsing of your
log files into BigQuery columns.

# Note: Mache is currently outdated

The current version of mache is outdated. It is going to be updated in the near future.

# How does it work?

App Engine provides access to logs for each request through the
[LogService API](https://developers.google.com/appengine/docs/java/logservice/).
The framework starts from a cron job that runs fairly often. The cron job simply enqueues a bunch of named tasks. These tasks are named such that one and only one task will run for a given time window. This time window is adjustable in your configuration class (see below). Every time this task runs, the task queries the log service for the logs for its specified time window, parses the logs into bigquery columns using built in extractor and/or extractors you write. It then exports this to bigquery using the streaming ingestion input method.


## Customizing the columns exported
The default implementation uses a set of user-defined exporters to parse each log
into a CSV line, which it then outputs to Google BigQuery.

The exporters to run are defined by a BigqueryFieldExporterSet. The framework
includes a set of default exporters that export most of the basic request log
information. See the "Writing your own exporter" section for the details of
adding exporters specific to your application's logs.

```
<cron>	
  <url>/bqlogging/logExportDirectToBigqueryStart?logsExportConfiguration=fullyqualifiedclassnamehere</url>
  <description>Export logs to BigQuery</description>
  <schedule>every 1 minutes</schedule> 
</cron>
```

# Customizing the export
## Parameters to the CRON task
- **logsExportConfiguration** specify a fully qualified class name for a class that implements the **LogsExportConfiguration** interface
## Writing your own exporter
You can export any field that your heart desires, as long as your heart desires one of the following data types:
 - **string** up to 64k
 - **integer**
 - **float**
 - **boolean**

You define fields to export by implementing com.streak.logging.analysis.BigqueryFieldExporter. It has the following methods that are run once for each log export to enumerate the schema:
 - **getFieldCount()** returns the number of fields parsed by this exporter.
 - **getFieldName(int)** takes an integer between 0 and getFieldCount() - 1, and returns the field name at that index. The ordering isn't important, but must be consistent with *getFieldType()*.
 - **getFieldType(int)** takes an integer between 0 and getFieldCount() - 1, and returns the field type at that index. The ordering isn't important, but must be consistent with *getFieldName()*. 

It also contains the following method that is run once per log entry:
 - **processLog(RequestLogs)** takes a com.google.appengine.api.log.RequestLogs instance and extracts the fields. It is followed by a set of *getField* calls to get the parsed fields.

After each call to *processLog(RequestLogs)*, the following method is called once for each field defined in the schema:
 - **getField(String)** returns the value for the given field name. The field name is guaranteed to be an interned string for efficient comparison. The return type should be appropriate to the data type you gave in *getFieldType*, but can be any object for which the *toString()* can be parsed appropriately by BigQuery (i.e. for an integer, either an Integer or a Long can be returned). If there is an error parsing the field, return null to abort the export. To indicate a lack of value, return an empty string.

In order to run your BigqueryFieldExporter, you will need to implement a com.streak.logging.analysis.BigqueryFieldExporterSet. It only has one method:
 - **getExporters()** returns the list of BigqueryFieldExporters.

Checkout the documentation in <code>LogsExportConfiguration</code>.

# Exporting Datastore Entities to BigQuery
We've been working on this functionality or a little bit of time but recently Google launched the ability for you to import datastore backups into BigQuery. The feature however is a manual process. Mache has built the ability for you to automatically kickoff backups of desired entity kinds and automatically start BigQuery ingestion jobs when the backup is complete. 

## Getting Started With Datastore to BigQuery Exports
1. Add the mache JAR to your project
2. Add the URL's listed in the logging section to your web.xml
3. Create a class which implements <code>BuiltinDatastoreExportConfiguration</code>
4. Call <code>/bqlogging/builtinDatastoreExport?builtinDatastoreExportConfig=</code><the fully qualified class name that you implemented>

You can put this call in your cron.xml to have the bigquery tables updated periodically. Checkout the documentation in <code>BuiltinDatastoreExportConfiguration</code>.

# Sample web.xml

```
	<servlet>
		<servlet-name>CreateLoggingTableServlet</servlet-name>
		<servlet-class>com.streak.logging.analysis.CreateLoggingTableServlet</servlet-class>
	</servlet>
	<servlet-mapping>
		<servlet-name>CreateLoggingTableServlet</servlet-name>
		<url-pattern>/bqlogging/createLoggingTable</url-pattern>
	</servlet-mapping>
	
	<servlet>
		<servlet-name>LogExportDirectToBigqueryTask</servlet-name>
		<servlet-class>com.streak.logging.analysis.LogExportDirectToBigqueryTask</servlet-class>
	</servlet>
	<servlet-mapping>
		<servlet-name>LogExportDirectToBigqueryTask</servlet-name>
		<url-pattern>/bqlogging/logExportDirectToBigqueryTask</url-pattern>
	</servlet-mapping>

	<servlet>
		<servlet-name>LogExportDirectToBigqueryStart</servlet-name>
		<servlet-class>com.streak.logging.analysis.LogExportDirectToBigqueryStart</servlet-class>
	</servlet>
	<servlet-mapping>
		<servlet-name>LogExportDirectToBigqueryStart</servlet-name>
		<url-pattern>/bqlogging/logExportDirectToBigqueryStart</url-pattern>
	</servlet-mapping>

	<servlet>
		<servlet-name>BigqueryStatusServlet</servlet-name>
		<servlet-class>com.streak.logging.analysis.BigqueryStatusServlet</servlet-class>
	</servlet>
	<servlet-mapping>
		<servlet-name>BigqueryStatusServlet</servlet-name>
		<url-pattern>/bqlogging/bigqueryStatus</url-pattern>
	</servlet-mapping>

	<servlet>
		<servlet-name>BuiltinDatastoreToBigqueryCronTask</servlet-name>
		<servlet-class>com.streak.datastore.analysis.builtin.BuiltinDatastoreToBigqueryCronTask</servlet-class>
	</servlet>
	<servlet-mapping>
		<servlet-name>BuiltinDatastoreToBigqueryCronTask</servlet-name>
		<url-pattern>/bqlogging/builtinDatastoreExport</url-pattern>
	</servlet-mapping>

	<servlet>
		<servlet-name>BuiltinDatastoreToBigqueryIngestorTask</servlet-name>
		<servlet-class>com.streak.datastore.analysis.builtin.BuiltinDatastoreToBigqueryIngesterTask</servlet-class>
	</servlet>
	<servlet-mapping>
		<servlet-name>BuiltinDatastoreToBigqueryIngestorTask</servlet-name>
		<url-pattern>/bqlogging/builtinDatastoreToBigqueryIngestorTask</url-pattern>
	</servlet-mapping>

	<security-constraint>
		<web-resource-collection>
			<url-pattern>/bqlogging/*</url-pattern>
		</web-resource-collection>
		<auth-constraint>
			<role-name>admin</role-name>
		</auth-constraint>
	</security-constraint>
```


# Registration
If you haven't already, sign up for BigQuery at https://code.google.com/apis/console/

You will need to enable billing under the Billing tab. 

## Register your App Engine app with BigQuery

1. Go to the Google APIs console at https://code.google.com/apis/console/
2. Go to the Team tab.
3. In the "Add a teammate:" field, enter {your appid}@appspot.gserviceaccount.com and give it owner permissions.

### Create your BigQuery dataset
1. Go to the BigQuery browser tool at https://bigquery.cloud.google.com/.
2. Choose the blue triangular dropdown next to the name of your Google APIs project.
3. Choose "Create new dataset". Note the name you chose. This is your BigQuery dataset ID.

### Get your Google APIs project ID
1. Go to the Google APIs console at https://code.google.com/apis/console/, select the Google Cloud Storage tab, and make note of the number following "x-goog-project-id:". This is your Goole APIs project ID.
