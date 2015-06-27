package com.streak.logging.dataflow;

import com.rewardly.mailfoo.utils.Constants;
import org.json.JSONArray;

import java.io.UnsupportedEncodingException;

public class LogUtils {

    public static final String KEY_VALUE_SEPERATOR = ": ";

//    public static void logNamedStructeredObject(String name, Object o, Level level) {
//        StringBuilder sb = new StringBuilder();
//        sb.append(LogUtils.getLogLineHeadingFor(name));
//        sb.append(new Gson().toJson(o));
//        Logger.log(level, sb.toString());
//    }
//
//    public static String extractParameter(String param, AppLogLine logLine) {
//        return extractParameter(param, logLine.getLogMessage());
//    }
//
    public static String extractParameter(String param, String msg) {
        String[] lines = msg.split("\\r?\\n");
        for (String line : lines) {
            if (line.startsWith(param + KEY_VALUE_SEPERATOR)) {
                return line.replace(param + KEY_VALUE_SEPERATOR, "");
            }
        }
        return "";
    }
//
//    public static String extractJson(AppLogLine logLine) {
//        return extractJson(logLine.getLogMessage());
//    }
//
//    public static String extractJson(String msg) {
//        if (msg.indexOf("{") > -1) {
//            return msg.substring(msg.indexOf("{"));
//        }
//        return "";
//    }
//
//    public static boolean isLineHeadingFor(String headingName, AppLogLine line) {
//        return isLineHeadingFor(headingName, line.getLogMessage());
//    }
//
//    public static boolean isLineHeading(AppLogLine line) {
//        return isLineHeading(line.getLogMessage());
//    }
//
    public static boolean isLineHeadingFor(String headingName, String line) {
        if (line.contains(getLogLineHeadingFor(headingName))) {
            return true;
        }
        return false;
    }

    public static String getLogLineHeadingFor(String headingName) {
        return headingName + Constants.LOG_LINE_HEADER_SUFFIX + "\n";
    }

    public static boolean isLineHeading(String line) {
        String[] lines = line.split("\n");
        if (lines != null && lines.length > 0 && lines[0].contains(Constants.LOG_LINE_HEADER_SUFFIX)) {
            return true;
        }
        return false;
    }

    public static int sizeOfString(String string) {
        try {
            byte[] currentBytes = string.getBytes("UTF-8");
            return currentBytes.length;
        }
        catch (UnsupportedEncodingException e) {
            return 0;
        }
    }

    public static String fullTextOfLog(JSONArray log, int byteLimit) {
        int bytesTotal = 0;
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < log.length(); i++) {
            String logLine = log.getJSONObject(i).getString("logMessage");
            if (LogUtils.isLineHeading(logLine)) {
                continue;
            }
            String toAppend = logLine + "\r\n";
            sb.append(toAppend);

            bytesTotal += LogUtils.sizeOfString(toAppend);
            if (bytesTotal > byteLimit) {
                //Logger.log("Hit byte limit for request: " + log.getRequestId()); TODO: figure out logging within dataflow
                break;
            }
        }
        return sb.toString();
    }
}
