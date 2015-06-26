package com.streak.logging.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.json.JSONObject;

/**
 * Created by lordprogrammer on 6/26/15.
 */
public interface FieldExporter {
    public Object getFieldValue(JSONObject log);

    public TableFieldSchema getSchema();
}
