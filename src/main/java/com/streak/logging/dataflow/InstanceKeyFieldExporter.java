package com.streak.logging.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.json.JSONObject;

/**
 * Created by lordprogrammer on 6/26/15.
 */
public class InstanceKeyFieldExporter implements FieldExporter {
    @Override
    public Object getFieldValue(JSONObject log) {
        return log.has("instanceId") ? log.getString("instanceId") : "";
    }

    @Override
    public TableFieldSchema getSchema() {
        return new TableFieldSchema().setName("instanceKey").setType("STRING");
    }
}
