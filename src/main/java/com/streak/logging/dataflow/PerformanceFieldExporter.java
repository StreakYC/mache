package com.streak.logging.dataflow;

import java.util.Arrays;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.json.JSONObject;

public class PerformanceFieldExporter implements LogsFieldExporter {
    private static final List<String> NAMES = Arrays.asList(
            "cost",
            "responseSize",
            "mcycles",
            "loadingRequest",
            "pendingTimeUsec",
            "latencyUsec");

    private static final List<String> TYPES = Arrays.asList(
            "FLOAT",   // cost
            "INTEGER", // responseSize
            "INTEGER", // mcycles
            "BOOLEAN", // loadingRequest
            "INTEGER", // pendingTimeUsec
            "INTEGER"  // latencyUsec
    );

    private long responseSize, mcycles, pendingTimeUsec, latencyUsec;
    private boolean loadingRequest;
    private double cost;

    @Override
    public void processLog(JSONObject log) {
        cost = log.has("cost") ? log.getDouble("cost") : 0;
        responseSize = log.getInt("responseSize");
        mcycles = log.has("megaCycles") ? log.getInt("megaCycles") : 0L;
        loadingRequest = log.has("wasLoadingRequest") ? log.getBoolean("wasLoadingRequest") : false;
        pendingTimeUsec = log.has("pendingTime") ? getUsecFromSecondsString(log.getString("pendingTime")) : 0L;
        latencyUsec = getUsecFromSecondsString(log.getString("latency"));
    }

    private long getUsecFromSecondsString(String seconds) {
        seconds = seconds.replace("s", "");
        double stupidVariable = Double.valueOf(seconds);
        return (long) (stupidVariable * 1000000);
    }

    @Override
    public Object getField(String name) {
        if (name == "cost") {
            return cost;
        }
        if (name == "responseSize") {
            return responseSize;
        }
        if (name == "mcycles") {
            return mcycles;
        }
        if (name == "loadingRequest") {
            return loadingRequest;
        }
        if (name == "pendingTimeUsec") {
            return pendingTimeUsec;
        }
        if (name == "latencyUsec") {
            return latencyUsec;
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
        return TYPES.get(i);
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
