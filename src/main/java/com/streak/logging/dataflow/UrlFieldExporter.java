package com.streak.logging.dataflow;

import java.util.Arrays;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.json.JSONObject;

public class UrlFieldExporter implements LogsFieldExporter {
    private static final List<String> NAMES = Arrays.asList("host", "path", "resource");

    String host = "";
    String path = "";
    String resource = "";

    @Override
    public void processLog(JSONObject log) {
        host = log.getString("host");
        resource = log.getString("resource");
        path = resource.indexOf("?") > -1 ? resource.substring(0, resource.indexOf("?")) : resource;
    }

    @Override
    public Object getField(String name) {
        if (name.equals("host")) {
            return host;
        }
        if (name.equals("path")) {
            return path;
        }
        if (name.equals("resource")) {
            return resource;
        }

        return null;
    }

    @Override
    public int getFieldCount() {
        return 3;
    }

    @Override
    public String getFieldName(int i) {
        return NAMES.get(i);
    }

    @Override
    public String getFieldType(int i) {
        return "STRING";
    }

    @Override
    public boolean getFieldNullable(int i) {
        return false;
    }

    @Override
    public boolean getFieldRepeated(int i) {
        return false;
    }

    @Override
    public String getFieldMode(int fieldIndex) {
        return "REQUIRED";
    }

    @Override
    public List<TableFieldSchema> getFieldFields(int i) {
        return null;
    }


}
