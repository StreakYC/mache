# What is this?

The Mache framework is a Java App Engine library that makes it easy
to export your App Engine application logs to BigQuery for analysis. It
consists of a cron task that periodically copies data from the App Engine
LogService to a BigQuery table, and lets you customize the parsing of your
log files into BigQuery columns.

# How does it work?

App Engine provides access to logs for each request through the
[LogService API](https://developers.google.com/appengine/docs/java/logservice/).
The framework starts from a cron job that runs fairly often (the timing is
adjustable but the default is every 2 minutes). The cron job looks at the
amount of time passed since the end of the last exported log entry, sections
the remaining logs into a series of timeslices of fixed duration (specified by
the *msPerFile* parameter) and starts the export process for each timeslice.

The export process has two phases, each of which runs as a separate
task queue task:

 - The first phase exports the logs from the App Engine LogService to
 a CSV file on Google Cloud Storage.
 - The second phase initiates a BigQuery job to load the CSV file
 into BigQuery.

In graphical form, we can see a sample set of cron runs below:

![Sample run graph](https://github.com/StreakYC/mache/raw/master/mache_diagram.png)

The graph has time of the logs being processed as the y-axis (running from top
to bottom), and the stage of processing as the x-axis (running from left to
right). Each cron job, represented by the leftmost column, looks at the time
elapsed since the last export. If enough time has passed (as defined by the
*msPerFile* parameter, which is set to 2000ms in this example) that it is time
to run a new export job, then the cron
task starts a new *storeLogsInCloudStorage* task to write the logs to a CSV
file. That task then starts a *loadCloudStorageToBigquery* task to load the
CSV files to BigQuery.

Multiple files are appended to the same table for a time period defined
by the *msPerTable* parameter, which is set to 4000ms in this example. If
*msPerFile* didn't divide *msPerTable*, then a single file would have to
be split into multiple tables (i.e. if *msPerFile* = 3000 and *msPerTable* =
4000, then the file encompassing 3s-6s would have to be divided between the
0s-4s and 4s-8s tables). This is currently not supported. For the full list
of restrictions on the *msPerTable* and *msPerFile* parameters, see the
"Changing the aggregation parameters" section.

## Exporting to Cloud Storage 
The initial task (StoreLogsInCloudStorageTask) iterates over each request log
from the LogService. It uses a set of user-defined exporters to parse each log
into a CSV line, which it then outputs to Google Cloud Storage. The timeslice
to output to each file is defined by the *msPerFile* variable. The tradeoff
is that lower values decrease the latency between a log event happening and
the event being exported to BigQuery, but increase the frequency of export
operations, which consume application resources. In any case, there is a
BigQuery limit of 1,000 loads/day, so *msPerFile* values of less than 2 minutes
may subject you to BigQuery rate limiting later in the day.

The exporters to run are defined by a BigqueryFieldExporterSet. The framework
includes a set of default exporters that export most of the basic request log
information. See the "Writing your own exporter" section for the details of
adding exporters specific to your application's logs.

## Loading to BigQuery
The second task (LoadCloudStorageToBigqueryTask) initiates a BigQuery load job
for the CSV file. Since the BigQuery clients currently make querying from
multiple tables somewhat painful, the framework provides support for aggregating
multiple files into a single table. The timeslice to aggregate into a single
table is defined by the *msPerTable* variable.

# Installation
 - Clone the repository from github:

```
$ git clone {insert repo url} 
```

 - Copy mache-0.0.1.jar to your project's war/WEB-INF/lib directory. Add the jar to your Eclipse build path.
 - If your project doesn't otherwise interact with BigQuery, you'll need to add it to project dependencies: Right click on your project, select Google->Add Google APIs->BigQuery API. Make sure you have the latest version of the Google Eclipse Plugin to enable this option.
 - Add the following snippet to your war/WEB-INF/web.xml file:

```
<servlet>
  <servlet-name>LogExportCronTask</servlet-name>
  <servlet-class>com.streak.logging.analysis.LogExportCronTask</servlet-class>
</servlet>
<servlet-mapping>
  <servlet-name>LogExportCronTask</servlet-name>
  <url-pattern>/logging/logExportCron</url-pattern>
</servlet-mapping>

<servlet>
  <servlet-name>StoreLogsInCloudStorageTask</servlet-name>
  <servlet-class>com.streak.logging.analysis.StoreLogsInCloudStorageTask</servlet-class>
</servlet>
<servlet-mapping>
  <servlet-name>StoreLogsInCloudStorageTask</servlet-name>
  <url-pattern>/logging/storeLogsInCloudStorage</url-pattern>
</servlet-mapping>

<servlet>
  <servlet-name>LoadCloudStorageToBigqueryTask</servlet-name>
  <servlet-class>com.streak.logging.analysis.LoadCloudStorageToBigqueryTask</servlet-class>
</servlet>
<servlet-mapping>
  <servlet-name>LoadCloudStorageToBigqueryTask</servlet-name>
  <url-pattern>/logging/loadCloudStorageToBigquery</url-pattern>
</servlet-mapping>

<servlet>
  <servlet-name>BigqueryStatusServlet</servlet-name>
  <servlet-class>com.streak.logging.analysis.BigqueryStatusServlet</servlet-class>
</servlet>
<servlet-mapping>
  <servlet-name>BigqueryStatusServlet</servlet-name>
  <url-pattern>/logging/bigqueryStatus</url-pattern>
</servlet-mapping>

<servlet>
  <servlet-name>DatastoreExportServlet</servlet-name>
  <servlet-class>com.streak.datastore.analysis.DatastoreExportServlet</servlet-class>
</servlet>
<servlet-mapping>
  <servlet-name>DatastoreExportServlet</servlet-name>
  <url-pattern>/logging/datastoreExport</url-pattern>
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
    <url-pattern>/logging/*</url-pattern>
  </web-resource-collection>
  <auth-constraint>
    <role-name>admin</role-name>
  </auth-constraint>
</security-constraint>
```

# Registration
If you haven't already, sign up for Cloud Storage and BigQuery at https://code.google.com/apis/console/

You will need to enable billing under the Billing tab. 

## Register your App Engine app with Cloud Storage and BigQuery
### Cloud Storage
Download gsutil following the instructions at https://developers.google.com/storage/docs/gsutil_install

If you haven't already, authorize gsutil to access your Google Cloud Storage account with:
```
$ gsutil config
```

Create a bucket with:
```
$ gsutil mb gs://{bucket name}
```

Dump the bucket's ACL to a file:
```
$ gsutil getacl gs://{bucket name} > bucket.acl
```

Add the following snippet before the first <Entry> tag in bucket.acl:
```
<Entry>
  <Scope type="UserByEmail">
     <EmailAddress>
        {your appid}@appspot.gserviceaccount.com
     </EmailAddress>
  </Scope>
  <Permission>
     WRITE
  </Permission>
</Entry>
```

Load the new ACL for the bucket:
```
$ gsutil setacl bucket.acl gs://{bucket name}
```

### BigQuery
1. Go to the Google APIs console at https://code.google.com/apis/console/
2. Go to the Team tab.
3. In the "Add a teammate:" field, enter {your appid}@appspot.gserviceaccount.com and give it owner permissions.

#### Create your BigQuery dataset
1. Go to the BigQuery browser tool at https://bigquery.cloud.google.com/.
2. Choose the blue triangular dropdown next to the name of your Google APIs project.
3. Choose "Create new dataset". Note the name you chose. This is your BigQuery dataset ID.

#### Get your Google APIs project ID
1. Go to the Google APIs console at https://code.google.com/apis/console/, select the Google Cloud Storage tab, and make note of the number following "x-goog-project-id:". This is your Goole APIs project ID.

# Test and Productionize
## Test the cron task
Test by going to:  
http://{your appid}.appspot.com/logging/logExportCron?bucketName={bucket name}&bigqueryProjectId={your Google APIs project id}&bigqueryDatasetId={your bigquery dataset id}

## Set the cron task to run regularly
Add an entry to your cron.xml file. If you change the *msPerFile* parameter (see the next section for details), you'll also want to change the frequency with which the cron task runs to match the parameter:

```
<cron>
  <url>/logging/logExportCron?bucketName={bucket name}&amp;bigqueryProjectId={your Google APIs project id}&amp;bigqueryDatasetId={your bigquery dataset id}</url>
  <description>Export logs to BigQuery</description>
  <schedule>every 2 minutes</schedule>
</cron>
```

# Customizing the export
## Parameters for logExportCron
Configuration of the export process is done through a series of parameters that are passed as query string arguments to the logExportCron servlet (see the previous section for a description of how to set up the cron.xml entry to pass these parameters). Note that you have to escape ampersands in cron.xml, but not if you're manually accessing the URL in your browser.

 - **bigqueryProjectId** the Google APIs project ID (a large integer) to use for BigQuery. Found in the API console. Required.
 - **bigqueryDatasetId** the BigQuery dataset ID (a user-defined string) to insert tables into. Default: logsdataset
 - **bigqueryFieldExporterSet** the **fully-qualified** class name of a BigqueryFieldExporterSet that defines the BigqueryFieldExporters to use to parse the logs. Default: com.streak.logging.analysis.example.BasicFieldExporterSet
 - **bucketName** the Cloud Storage bucket to use for csv files. Required.
 - **queueName** the Google App Engine queue to use for exporter tasks. Defaults to the default Google App Engine queue.
 - **msPerFile** the number of milliseconds worth of logs that should be aggregated in each csv file. This should be an amount that can be processed in the 10 minute offline task request limit. You should also update your cron.xml entry to run at this interval. This must divide *msPerTable*. **See the "Changing the aggregation parameters" section before changing msPerFile or msPerTable or you may lose data**. Default: 120000 (= 2 mins).
 - **msPerTable** the number of milliseconds worth of logs that should be aggregated in each BigQuery table. Default: 86400000 (= 1 day)
 - **logLevel** the minimum log level to export. One of: ALL, DEBUG, ERROR, FATAL, INFO, or WARN. Default: ALL

Most parameters can also be set by changing the appropriate getDefault method in LogExportCronTask and recompiling the jar if you prefer.

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

In order to run your BigqueryFieldExporter, you will need to imlement a com.streak.logging.analysis.BigqueryFieldExporterSet. It only has one method:
 - **getExporters()** returns the list of BigqueryFieldExporters.

You then pass the fully-qualified classname of the BigqueryFieldExporterSet as the *bigqueryFieldExporterSet* parameter to cron URL. For an example of the exporters, look at the exporters in the com.streak.logging.analysis.example package. 

## Changing the aggregation parameters
Currently, splitting a single file between multiple tables is not supported, so
*msPerFile* must evenly divide *msPerTable*.

In general, if you change *msPerTable* or *msPerFile* it may result in logs
being omitted or duplicated in exports until *msPerTable* time has passed.
The only currently supported change that will not lose data is changing
*msPerFile* to a value that divides its previous value.

# Building/Contributing
The Eclipse project will automatically use your installed App Engine SDK.
To build the jar, add your App Engine SDK directory to edit-to-build.properties,
and rename it to build.properties. Run ant to build.

# Exporting Datastore Entities to BigQuery
We've been working on this functionality or a little bit of time but recently Google launched the ability for you to import datastore backups into BigQuery. The feature however is a manual process. Mache has built the ability for you to automatically kickoff backups of desired entity kinds and automatically start BigQuery ingestion jobs when the backup is complete. 

## Getting Started With Datastore to BigQuery Exports
1. Add the mache JAR to your project
2. Add the URL's listed in the logging section to your web.xml
3. Create a class which implements <code>BuiltinDatastoreExportConfiguration</code>
4. Call <code>/bqlogging/builtinDatastoreExport?builtinDatastoreExportConfig=</code><the fully qualified class name that you implemented>

You can put this call in your cron.xml to have the bigquery tables updated periodically. Checkout the documentation in <code>BuiltinDatastoreExportConfiguration</code>.
