package com.streak.logging.dataflow;

import java.util.Arrays;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.json.JSONObject;

public class HttpTransactionFieldExporter implements LogsFieldExporter {
    private static final List<String> NAMES = Arrays.asList(
            "httpStatus", "method", "httpVersion", "requestId");

    private int httpStatus;
    private String method;
    private String httpVersion;
    private String requestId;

    @Override
    public void processLog(JSONObject log) {
        httpStatus = log.has("status") ? log.getInt("status") : 0;
        method = log.getString("method");
        httpVersion = log.getString("httpVersion");
        requestId = log.getString("requestId");
    }

    @Override
    public Object getField(String name) {
        if (name == "httpStatus") {
            return httpStatus;
        }
        if (name == "method") {
            return method;
        }
        if (name == "httpVersion") {
            return httpVersion;
        }
        if (name == "requestId") {
            return requestId;
        }

        return null;
    }

    @Override
    public int getFieldCount() {
        return NAMES.size();
    }

    @Override
    public String getFieldName(int i) {
        return NAMES.get(i);
    }

    @Override
    public String getFieldType(int i) {
        if (i == 0) {
            return "INTEGER";
        }
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
