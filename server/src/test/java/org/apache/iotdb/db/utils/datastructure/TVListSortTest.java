package org.apache.iotdb.db.utils.datastructure;

import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;

public class TVListSortTest {
  @Test
  public void testOrdered() {
    System.out.print("Test totally ordered:\n");
    String filepath = "D:\\Code\\iotdb\\memtable\\synthetic\\lognormal\\n_6_mu_0_sigma_0.csv";
    testSortAlgorithm(filepath);
    filepath = "D:\\Code\\iotdb\\memtable\\synthetic\\lognormal\\n_6_mu_0_sigma_0_error.csv";
    testSortAlgorithm(filepath);
  }

  @Test
  public void testSynthesisLogNormal() {
    System.out.print("Test LogNormal ordered:\n");
    String filepath = "D:\\Code\\iotdb\\memtable\\synthetic\\lognormal\\n_6_mu_%d_sigma_%.1f.csv";
    testSortAlgorithm(String.format(filepath, 0, 0.1));
    testSortAlgorithm(String.format(filepath, 0, 0.2));
    testSortAlgorithm(String.format(filepath, 0, 0.5));
    filepath = "D:\\Code\\iotdb\\memtable\\synthetic\\lognormal\\n_6_mu_%d_sigma_%d.csv";
    testSortAlgorithm(String.format(filepath, 0, 1));
    testSortAlgorithm(String.format(filepath, 0, 2));
    testSortAlgorithm(String.format(filepath, 0, 3));
    testSortAlgorithm(String.format(filepath, 0, 4));

    testSortAlgorithm(String.format(filepath, 1, 1));
    testSortAlgorithm(String.format(filepath, 1, 2));
    testSortAlgorithm(String.format(filepath, 2, 2));
  }

  @Test
  public void testRealDatasets() {
    System.out.print("testRealDatasets");
    String filePath;
    filePath = "D:\\Code\\iotdb\\memtable\\datasets\\sumsung\\d-5_1e6.csv";
    testSortAlgorithm(filePath);
    filePath = "D:\\Code\\iotdb\\memtable\\datasets\\sumsung\\s-10_1e6.csv";
    testSortAlgorithm(filePath);
    //          filePath = "D:\\Code\\iotdb\\memtable\\datasets\\Normal Traffic\\water_level.csv";
    //          filePath = "D:\\Code\\iotdb\\memtable\\datasets\\intel\\data_1e6.csv";
    filePath = "D:\\Code\\iotdb\\memtable\\datasets\\citibike\\201809.csv";
    testSortAlgorithm(filePath);
    filePath = "D:\\Code\\iotdb\\memtable\\datasets\\citibike\\201812.csv";
    testSortAlgorithm(filePath);
//    filePath = "D:\\Code\\iotdb\\memtable\\datasets\\tianyuan\\tianyuan.csv";
//    testSortAlgorithm(filePath);
  }

  public void testSortAlgorithm(String filepath) {
    System.out.printf("============================filepath=%s======================\n", filepath);
    HashMap<String, List> data = new HashMap<>();
    try {
      data = CsvReader.readCSVFromLong(filepath, 100000);
    } catch (IOException | ParseException e) {
      System.out.print("parse false");
    }
    List<Long> timestamps = data.get("timestamps");
    List<Integer> values = data.get("value");
    int N = timestamps.size();
    double inversions = 0;
    for (int i = 0; i < N - 1; i++) {
      if (timestamps.get(i) > timestamps.get(i + 1)) {
        inversions += 1;
      }
    }
    System.out.printf(
        "length=%d, inversions=%d, rates=%.2f\n", N, (int) inversions, inversions / N);

    BackIntTVList preTVList = new BackIntTVList();

    BackIntTVList backwardIntTVList = new BackIntTVList();
    TimIntTVList timIntTVList = new TimIntTVList();
    QuickIntTVList quickIntTVList = new QuickIntTVList();
    for (int k = 0; k < 1; k++) {
      for (int i = 0; i < N; i++) {
        long t = timestamps.get(i);
        int v = values.get(i);
        preTVList.putInt(t, v);
        backwardIntTVList.putInt(t, v);
        timIntTVList.putInt(t, v);
        quickIntTVList.putInt(t, v);
      }
    }
    double begin, end;
    // preTVList.sort();
    begin = System.currentTimeMillis();
    backwardIntTVList.sort();
    end = System.currentTimeMillis();
    System.out.printf("Backward, %.2f\n", end - begin);

    begin = System.currentTimeMillis();
    quickIntTVList.sort();
    end = System.currentTimeMillis();
    System.out.printf("Quick, %.2f\n", end - begin);

    begin = System.currentTimeMillis();
    timIntTVList.sort();
    end = System.currentTimeMillis();
    System.out.printf("Tim, %.2f\n", end - begin);

    for (int i = backwardIntTVList.rowCount - 1; i >= 0; i--) {
      if (backwardIntTVList.getTime(i) != timIntTVList.getTime(i)) {
        System.out.printf(
            "1 i=%d %d %d\n ", i, backwardIntTVList.getTime(i), timIntTVList.getTime(i));
        break;
      }
      if (quickIntTVList.getTime(i) != timIntTVList.getTime(i)) {
        System.out.printf("2 i=%d\n", i);
      }
    }
  }
}
