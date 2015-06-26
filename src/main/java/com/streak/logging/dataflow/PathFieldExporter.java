package com.streak.logging.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.json.JSONObject;

/**
 * Created by lordprogrammer on 6/26/15.
 */
public class PathFieldExporter implements FieldExporter {
    @Override
    public Object getFieldValue(JSONObject log) {
        String resource = log.getString("resource");
        return resource.indexOf("?") > -1 ? resource.substring(0, resource.indexOf("?")) : resource;
    }

    @Override
    public TableFieldSchema getSchema() {
        return new TableFieldSchema().setName("path").setType("STRING");
    }
}
