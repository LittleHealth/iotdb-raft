package org.apache.iotdb.db.utils.datastructure;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class CsvReader {
  public static final String[] STRING_TIME_FORMAT;

  static {
    STRING_TIME_FORMAT =
        new String[] {
          "yyyy-MM-dd HH:mm:ss.SSSX",
          "yyyy/MM/dd HH:mm:ss.SSSX",
          "yyyy.MM.dd HH:mm:ss.SSSX",
          "yyyy-MM-dd HH:mm:ssX",
          "yyyy/MM/dd HH:mm:ssX",
          "yyyy.MM.dd HH:mm:ssX",
          "yyyy-MM-dd HH:mm:ss.SSSz",
          "yyyy/MM/dd HH:mm:ss.SSSz",
          "yyyy.MM.dd HH:mm:ss.SSSz",
          "yyyy-MM-dd HH:mm:ssz",
          "yyyy/MM/dd HH:mm:ssz",
          "yyyy.MM.dd HH:mm:ssz",
          "yyyy-MM-dd HH:mm:ss.SSS",
          "yyyy/MM/dd HH:mm:ss.SSS",
          "yyyy.MM.dd HH:mm:ss.SSS",
          "yyyy-MM-dd HH:mm:ss",
          "yyyy/MM/dd HH:mm:ss",
          "yyyy.MM.dd HH:mm:ss",
          "yyyy-MM-dd'T'HH:mm:ss.SSSX",
          "yyyy/MM/dd'T'HH:mm:ss.SSSX",
          "yyyy.MM.dd'T'HH:mm:ss.SSSX",
          "yyyy-MM-dd'T'HH:mm:ssX",
          "yyyy/MM/dd'T'HH:mm:ssX",
          "yyyy.MM.dd'T'HH:mm:ssX",
          "yyyy-MM-dd'T'HH:mm:ss.SSSz",
          "yyyy/MM/dd'T'HH:mm:ss.SSSz",
          "yyyy.MM.dd'T'HH:mm:ss.SSSz",
          "yyyy-MM-dd'T'HH:mm:ssz",
          "yyyy/MM/dd'T'HH:mm:ssz",
          "yyyy.MM.dd'T'HH:mm:ssz",
          "yyyy-MM-dd'T'HH:mm:ss.SSS",
          "yyyy/MM/dd'T'HH:mm:ss.SSS",
          "yyyy.MM.dd'T'HH:mm:ss.SSS",
          "yyyy-MM-dd'T'HH:mm:ss",
          "yyyy/MM/dd'T'HH:mm:ss",
          "yyyy.MM.dd'T'HH:mm:ss"
        };
  }

  public static HashMap<String, List> readCSVFromLong(String filePath, int threshold)
          throws ParseException, IOException {
    CSVParser csvRecords =
            CSVFormat.Builder.create(CSVFormat.DEFAULT)
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setQuote('`')
                    .setEscape('\\')
                    .setIgnoreEmptyLines(true)
                    .build()
                    .parse(new InputStreamReader(new FileInputStream(filePath)));

    Stream<CSVRecord> records = csvRecords.stream();
    Object[] objects = records.toArray();

    int timeColumnIndex = 0;
    List<Long> timestamps = new ArrayList<>();
    List<Integer> values = new ArrayList<>();
    int cnt = 0;
    for (Object o : objects) {
      CSVRecord object = (CSVRecord) o;
      List<String> vals = object.toList();
      long timestamp = Long.parseLong(vals.get(timeColumnIndex));
      timestamps.add(timestamp);
      int value = cnt;
      cnt += 1;
      values.add(value);
      if (timestamps.size() >= threshold) break;
    }
    HashMap<String, List> data = new HashMap<>();
    data.put("timestamps", timestamps);
    data.put("value", values);
    return data;
  }


  /** not only read data from csv, but also parse the timestamps into `long` */
  public static HashMap<String, List> readCSV(String filePath, int threshold)
      throws ParseException, IOException {
    CSVParser csvRecords =
        CSVFormat.Builder.create(CSVFormat.DEFAULT)
            .setHeader()
            .setSkipHeaderRecord(true)
            .setQuote('`')
            .setEscape('\\')
            .setIgnoreEmptyLines(true)
            .build()
            .parse(new InputStreamReader(new FileInputStream(filePath)));

    List<String> headerNames = csvRecords.getHeaderNames();
    Stream<CSVRecord> records = csvRecords.stream();
    Object[] objects = records.toArray();
    String regex = "(?<=\\()\\S+(?=\\))";
    Pattern pattern = Pattern.compile(regex);

    int timeColumnIndex = 0;
    int intColumnIndex = 1;
    for (int i = 0; i < headerNames.size(); i++) {
      String headerName = headerNames.get(i);
      if ("Time".equalsIgnoreCase(headerName)) {
        timeColumnIndex = i;
        continue;
      }
      Matcher matcher = pattern.matcher(headerName);
      String type;
      if (matcher.find()) {
        type = matcher.group();
        if ("INT32".equalsIgnoreCase(type)) {
          intColumnIndex = i;
        }
      }
    }
    AtomicReference<SimpleDateFormat> timeFormatter = new AtomicReference<>(null);
    timeFormatter.set(formatterInit("2022-02-02 02:02:22"));
    List<Long> timestamps = new ArrayList<>();
    List<Integer> values = new ArrayList<>();
    for (Object o : objects) {
      CSVRecord object = (CSVRecord) o;
      List<String> vals = object.toList();
      long time = timeFormatter.get().parse(vals.get(timeColumnIndex)).getTime();
      timestamps.add(time + 30000000);
      int value = Integer.parseInt(vals.get(intColumnIndex));
      values.add(value);
      if (timestamps.size() >= threshold) break;
    }
    HashMap<String, List> data = new HashMap<>();
    data.put("timestamps", timestamps);
    data.put("value", values);
    return data;
  }

  public static SimpleDateFormat formatterInit(String time) {
    try {
      Long.parseLong(time);
      return null;
    } catch (Exception ignored) {
      // do nothing
    }

    for (String timeFormat : STRING_TIME_FORMAT) {
      SimpleDateFormat format = new SimpleDateFormat(timeFormat);
      try {
        format.parse(time);
        return format;
      } catch (java.text.ParseException ignored) {
        // do nothing
      }
    }
    return null;
  }
}
