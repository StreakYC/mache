package com.streak.logging.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.json.JSONObject;

/**
 * Created by lordprogrammer on 6/26/15.
 */
public class MethodFieldExporter implements FieldExporter {
    @Override
    public Object getFieldValue(JSONObject log) {
        return log.getString("method");
    }

    @Override
    public TableFieldSchema getSchema() {
        return new TableFieldSchema().setName("method").setType("STRING");
    }
}
