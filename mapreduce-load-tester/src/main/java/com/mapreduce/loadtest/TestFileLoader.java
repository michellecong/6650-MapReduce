package com.mapreduce.loadtest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class to load test files from directory
 */
public class TestFileLoader {
  private final String testFilesDirectory;
  private final List<TestFile> testFiles;
  private final Map<String, Map<String, Integer>> expectedResults;

  /**
   * Inner class for test files
   */
  public static class TestFile {
    private final String fileName;
    private final String content;

    public TestFile(String fileName, String content) {
      this.fileName = fileName;
      this.content = content;
    }

    public String getFileName() { return fileName; }
    public String getContent() { return content; }
  }

  /**
   * Constructor that loads test files from the specified directory
   */
  public TestFileLoader(String testFilesDirectory) throws IOException {
    this.testFilesDirectory = testFilesDirectory;
    this.testFiles = new ArrayList<>();
    this.expectedResults = new HashMap<>();

    loadTestFiles();
  }

  /**
   * Load all text files from the directory
   */
  private void loadTestFiles() throws IOException {
    File directory = new File(testFilesDirectory);

    if (!directory.exists() || !directory.isDirectory()) {
      throw new IOException("Test files directory does not exist or is not a directory: " + testFilesDirectory);
    }

    // First, find all text files (excluding .expected.csv files)
    List<File> textFiles = new ArrayList<>();
    try (Stream<Path> paths = Files.walk(Paths.get(testFilesDirectory))) {
      List<Path> allFiles = paths
          .filter(Files::isRegularFile)
          .collect(Collectors.toList());

      for (Path path : allFiles) {
        String filename = path.getFileName().toString();
        if (filename.endsWith(".txt") || filename.endsWith(".text")) {
          textFiles.add(path.toFile());
        }
      }
    }

    // For each text file, read its content and expected results (if available)
    for (File file : textFiles) {
      String fileName = file.getName();

      // Read text file content
      String content = readFileContent(file);
      testFiles.add(new TestFile(fileName, content));

      // Check if expected results file exists
      File expectedFile = new File(file.getParentFile(), fileName + ".expected.csv");
      if (expectedFile.exists()) {
        Map<String, Integer> wordCounts = readExpectedWordCounts(expectedFile);
        expectedResults.put(fileName, wordCounts);
      }
    }

    System.out.println("Loaded " + testFiles.size() + " test files from " + testFilesDirectory);
    for (TestFile file : testFiles) {
      System.out.println(" - " + file.getFileName() + " (" + file.getContent().length() + " bytes)");
    }
  }

  /**
   * Read file content as a string
   */
  private String readFileContent(File file) throws IOException {
    StringBuilder content = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = reader.readLine()) != null) {
        content.append(line).append("\n");
      }
    }
    return content.toString();
  }

  /**
   * Read expected word counts from CSV file
   */
  private Map<String, Integer> readExpectedWordCounts(File file) throws IOException {
    Map<String, Integer> wordCounts = new HashMap<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String line;
      boolean isHeader = true;

      while ((line = reader.readLine()) != null) {
        if (isHeader) {
          isHeader = false;
          continue;
        }

        String[] parts = line.split(",");
        if (parts.length >= 2) {
          String word = parts[0].trim();
          int count = Integer.parseInt(parts[1].trim());
          wordCounts.put(word, count);
        }
      }
    }
    return wordCounts;
  }

  /**
   * Get all loaded test files
   */
  public List<TestFile> getTestFiles() {
    return testFiles;
  }

  /**
   * Get expected results map
   */
  public Map<String, Map<String, Integer>> getExpectedResults() {
    return expectedResults;
  }

  /**
   * Get a random test file
   */
  public TestFile getRandomTestFile() {
    if (testFiles.isEmpty()) {
      return null;
    }
    return testFiles.get((int) (Math.random() * testFiles.size()));
  }
}