package com.streak.logging.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A streaming Dataflow Example using BigQuery output.
 *
 * <p> This pipeline example reads lines of text from a PubSub topic, splits each line
 * into individual words, capitalizes those words, and writes the output to
 * a BigQuery table.
 *
 * <p> By default, the example will run a separate pipeline to inject the data from the default
 * {@literal --inputFile} to the Pub/Sub {@literal --pubsubTopic}. It will make it available for
 * the streaming pipeline to process. You may override the default {@literal --inputFile} with the
 * file of your choosing. You may also set {@literal --inputFile} to an empty string, which will
 * disable the automatic Pub/Sub injection, and allow you to use separate tool to control the input
 * to this example.
 *
 * <p> The example is configured to use the default Pub/Sub topic and the default BigQuery table
 * from the example common package (there are no defaults for a general Dataflow pipeline).
 * You can override them by using the {@literal --pubsubTopic}, {@literal --bigQueryDataset}, and
 * {@literal --bigQueryTable} options. If the Pub/Sub topic or the BigQuery table do not exist,
 * the example will try to create them.
 *
 * <p> The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */
public class App {
    private static List<FieldExporter> exporters = Arrays.asList(new HttpStatusFieldExporter(), new MethodFieldExporter(), new HttpVersionFieldExporter(), new RequestIdFieldExporter(), new HostFieldExporter(), new ResourceFieldExporter(), new PathFieldExporter(), new CostFieldExporter(), new ResponseSizeFieldExporter(), new MegaCycleFieldExporter(), new LoadingRequestFieldExporter(), new PendingTimeUsecFieldExporter(), new LatencyUsecFieldExporter(), new TimestampFieldExporter(), new NicknameFieldExporter(), new IpAddressFieldExporter(), new UserAgentFieldExporter(), new InstanceKeyFieldExporter(), new VersionIdFieldExporter(), new ModuleIdFieldExporter(), new TraceFieldExporter());

    /**
     * Converts strings into BigQuery rows.
     */
    static class StringToRowConverter extends DoFn<String, TableRow> {
        private static final long serialVersionUID = 0;

        /**
         * In this example, put the whole string into single BigQuery field.
         */
        @Override
        public void processElement(ProcessContext c) {
            String rawInput = c.element();
            JSONObject log = new JSONObject(rawInput);
            TableRow tr = new TableRow().set("raw", rawInput);

            try {
                if (log.has("protoPayload") && log.getJSONObject("protoPayload").has("@type") && log.getJSONObject("protoPayload").getString("@type").equals("type.googleapis.com/google.appengine.logging.v1.RequestLog")) {
                    JSONObject innerLog = log.getJSONObject("protoPayload");
                    for (FieldExporter exporter : exporters) {
                        tr = tr.set(exporter.getSchema().getName(), exporter.getFieldValue(innerLog));
                    }
                }
            }
            catch (Exception e) {
                tr = tr.set("parseError", e.getMessage()).set("parseStack", Arrays.toString(e.getStackTrace()));
            }
            c.output(tr);
        }

        static TableSchema getSchema() {
            return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
                private static final long serialVersionUID = 0;

                // Compose the list of TableFieldSchema from tableSchema.
                {
                    add(new TableFieldSchema().setName("raw").setType("STRING"));
                    add(new TableFieldSchema().setName("parseError").setType("STRING"));
                    add(new TableFieldSchema().setName("parseStack").setType("STRING"));
                    for (FieldExporter exporter : exporters) {
                        add(exporter.getSchema());
                    }
                }
            });
        }
    }

    /**
     * Sets up and starts streaming pipeline.
     *
     * @throws IOException if there is a problem setting up resources
     */
    public static void main(String[] args) throws IOException {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(PubsubIO.Read.topic("/topics/mailfoogae/logstest1"))
                .apply(ParDo.of(new StringToRowConverter()))
                .apply(BigQueryIO.Write.to("mailfoogae:dataflowLogsTest.test14")
                        .withSchema(StringToRowConverter.getSchema()));

        pipeline.run();
    }
}
