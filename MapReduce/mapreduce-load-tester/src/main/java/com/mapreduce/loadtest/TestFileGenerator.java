package com.mapreduce.loadtest;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Utility class to generate test files with known word frequencies for testing
 */
public class TestFileGenerator {
  private static final Random random = new Random();
  private static final String[] COMMON_WORDS = {
      "the", "of", "and", "a", "to", "in", "is", "you", "that", "it", "he", "was", "for", "on", "are", "as",
      "with", "his", "they", "i", "at", "be", "this", "have", "from", "or", "one", "had", "by", "word", "but",
      "not", "what", "all", "were", "we", "when", "your", "can", "said", "there", "use", "an", "each", "which",
      "she", "do", "how", "their", "if", "will", "up", "other", "about", "out", "many", "then", "them", "these",
      "so", "some", "her", "would", "make", "like", "him", "into", "time", "has", "look", "two", "more", "write",
      "go", "see", "number", "no", "way", "could", "people", "my", "than", "first", "water", "been", "call",
      "who", "oil", "its", "now", "find", "long", "down", "day", "did", "get", "come", "made", "may", "part"
  };

  // Specialized words for technical content
  private static final String[] TECH_WORDS = {
      "mapreduce", "hadoop", "java", "programming", "code", "algorithm", "system", "distributed", "parallel",
      "processing", "data", "cluster", "node", "server", "client", "compute", "job", "task", "performance",
      "optimization", "latency", "throughput", "network", "storage", "memory", "cpu", "hardware", "software",
      "implementation", "framework", "library", "api", "function", "method", "class", "object", "interface",
      "database", "query", "index", "cache", "stream", "file", "directory", "path", "cloud", "container",
      "virtualization", "scalability", "reliability", "availability", "fault","tolerance", "monitoring"
  };

  // Output directory for generated test files
  private static final String OUTPUT_DIR = "test-files";

  public static void main(String[] args) throws IOException {
    // Create output directory if it doesn't exist
    Files.createDirectories(Paths.get(OUTPUT_DIR));

    // Generate a file of approximately 3KB
    generateFileWithSpecificSize("file_3kb.txt", 3);

    // Generate small test files
//    generateSmallTestFile("small_text.txt", 50, 10);
//    generateSmallTestFile("medium_text.txt", 200, 30);
//    generateSmallTestFile("large_text.txt", 1000, 100);

    // Generate technical content
//    generateTechnicalContent("mapreduce_article.txt", 500);

    // Generate file with known distribution
//    generateFileWithKnownDistribution("known_distribution.txt");

    System.out.println("Test files generated successfully in the '" + OUTPUT_DIR + "' directory.");
  }

  /**
   * Generate a file with specific size (approximately)
   */
  private static void generateFileWithSpecificSize(String fileName, int targetSizeKB) throws IOException {
    // 1KB is approximately 1000 characters
    // An average English word plus space is about 6 characters
    // So 3KB needs around 500 words
    int estimatedWordCount = targetSizeKB * 170;  // Estimated value, might need adjustment

    random.setSeed(123); // For reproducibility
    StringBuilder content = new StringBuilder();
    Map<String, Integer> wordFrequency = new HashMap<>();

    // Mix common and technical words
    String[] allWords = new String[COMMON_WORDS.length + TECH_WORDS.length];
    System.arraycopy(COMMON_WORDS, 0, allWords, 0, COMMON_WORDS.length);
    System.arraycopy(TECH_WORDS, 0, allWords, COMMON_WORDS.length, TECH_WORDS.length);

    int wordsAdded = 0;
    int wordsPerLine = 10; // 每行包含10个单词，你可以调整这个数字
    int currentLineWordCount = 0;

    while (wordsAdded < estimatedWordCount) {
      String word = allWords[random.nextInt(allWords.length)];
      content.append(word).append(" ");
      wordFrequency.put(word, wordFrequency.getOrDefault(word, 0) + 1);
      wordsAdded++;
      currentLineWordCount++;

      // 每行添加指定数量的单词后添加换行符
      if (currentLineWordCount >= wordsPerLine) {
        content.append(System.lineSeparator());
        currentLineWordCount = 0;
      }

      // Add some sentence structure (句号后可能跟换行)
      if (wordsAdded % 12 == 0) {
        content.append(". ");

        // 有一定概率在句号后添加换行
        if (random.nextDouble() < 0.3) { // 30%的概率添加换行
          content.append(System.lineSeparator());
          currentLineWordCount = 0;
        }

        // Capitalize next word if not at the end
        if (wordsAdded < estimatedWordCount) {
          String nextWord = allWords[random.nextInt(allWords.length)];
          nextWord = Character.toUpperCase(nextWord.charAt(0)) + nextWord.substring(1);
          content.append(nextWord).append(" ");

          // Track frequency (convert to lowercase for consistent counting)
          String lowercaseWord = nextWord.toLowerCase();
          wordFrequency.put(lowercaseWord, wordFrequency.getOrDefault(lowercaseWord, 0) + 1);
          wordsAdded++;
          currentLineWordCount++;
        }
      }
    }

    // Write content to file
    try (FileWriter writer = new FileWriter(new File(OUTPUT_DIR, fileName))) {
      writer.write(content.toString());
    }

    // Check file size
    File file = new File(OUTPUT_DIR, fileName);
    double actualSizeKB = file.length() / 1024.0;
    System.out.println("Generated file " + fileName + " with size: " + String.format("%.2f", actualSizeKB) + " KB");
    System.out.println("Generated file contains multiple lines for better chunk testing");

    // Write expected word counts to a companion file
    try (FileWriter writer = new FileWriter(new File(OUTPUT_DIR, fileName + ".expected.csv"))) {
      writer.write("word,count\n");
      for (Map.Entry<String, Integer> entry : wordFrequency.entrySet()) {
        writer.write(entry.getKey() + "," + entry.getValue() + "\n");
      }
    }
  }

  /**
   * Generate a small test file with random content
   */
  private static void generateSmallTestFile(String fileName, int wordCount, int randomSeed) throws IOException {
    random.setSeed(randomSeed); // For reproducibility
    StringBuilder content = new StringBuilder();
    Map<String, Integer> wordFrequency = new HashMap<>();

    for (int i = 0; i < wordCount; i++) {
      String word = COMMON_WORDS[random.nextInt(COMMON_WORDS.length)];
      content.append(word).append(" ");

      // Track frequency
      wordFrequency.put(word, wordFrequency.getOrDefault(word, 0) + 1);

      // Add some sentence structure
      if (i > 0 && i % 15 == 0) {
        content.append(". ");
        // Capitalize next word
        if (i < wordCount - 1) {
          String nextWord = COMMON_WORDS[random.nextInt(COMMON_WORDS.length)];
          nextWord = Character.toUpperCase(nextWord.charAt(0)) + nextWord.substring(1);
          content.append(nextWord).append(" ");
          i++; // Count this word

          // Track frequency (convert to lowercase for consistent counting)
          String lowercaseWord = nextWord.toLowerCase();
          wordFrequency.put(lowercaseWord, wordFrequency.getOrDefault(lowercaseWord, 0) + 1);
        }
      }
    }

    // Write content to file
    try (FileWriter writer = new FileWriter(new File(OUTPUT_DIR, fileName))) {
      writer.write(content.toString());
    }

    // Write expected word counts to a companion file
    try (FileWriter writer = new FileWriter(new File(OUTPUT_DIR, fileName + ".expected.csv"))) {
      writer.write("word,count\n");
      for (Map.Entry<String, Integer> entry : wordFrequency.entrySet()) {
        writer.write(entry.getKey() + "," + entry.getValue() + "\n");
      }
    }
  }

  /**
   * Generate technical content related to MapReduce
   */
  private static void generateTechnicalContent(String fileName, int wordCount) throws IOException {
    random.setSeed(42); // For reproducibility
    StringBuilder content = new StringBuilder();
    Map<String, Integer> wordFrequency = new HashMap<>();

    // Start with a topic sentence
    String intro = "MapReduce is a programming model for processing and generating large data sets. ";
    content.append(intro);

    // Count words in the intro
    for (String word : intro.toLowerCase().split("\\s+")) {
      word = word.replaceAll("[^a-zA-Z]", ""); // Remove non-alphabetic characters
      if (!word.isEmpty()) {
        wordFrequency.put(word, wordFrequency.getOrDefault(word, 0) + 1);
      }
    }

    int wordsAdded = intro.split("\\s+").length;

    while (wordsAdded < wordCount) {
      // Decide whether to use a common word or technical word
      String word;
      if (random.nextDouble() < 0.7) { // 70% common words
        word = COMMON_WORDS[random.nextInt(COMMON_WORDS.length)];
      } else { // 30% technical words
        word = TECH_WORDS[random.nextInt(TECH_WORDS.length)];
      }

      content.append(word).append(" ");
      wordFrequency.put(word, wordFrequency.getOrDefault(word, 0) + 1);
      wordsAdded++;

      // Add some sentence structure
      if (wordsAdded % 12 == 0) {
        content.append(". ");
        // Capitalize next word if not at the end
        if (wordsAdded < wordCount) {
          String nextWord;
          if (random.nextDouble() < 0.7) {
            nextWord = COMMON_WORDS[random.nextInt(COMMON_WORDS.length)];
          } else {
            nextWord = TECH_WORDS[random.nextInt(TECH_WORDS.length)];
          }
          nextWord = Character.toUpperCase(nextWord.charAt(0)) + nextWord.substring(1);
          content.append(nextWord).append(" ");

          // Track frequency (convert to lowercase for consistent counting)
          String lowercaseWord = nextWord.toLowerCase();
          wordFrequency.put(lowercaseWord, wordFrequency.getOrDefault(lowercaseWord, 0) + 1);
          wordsAdded++;
        }
      }
    }

    // Write content to file
    try (FileWriter writer = new FileWriter(new File(OUTPUT_DIR, fileName))) {
      writer.write(content.toString());
    }

    // Write expected word counts to a companion file
    try (FileWriter writer = new FileWriter(new File(OUTPUT_DIR, fileName + ".expected.csv"))) {
      writer.write("word,count\n");
      for (Map.Entry<String, Integer> entry : wordFrequency.entrySet()) {
        writer.write(entry.getKey() + "," + entry.getValue() + "\n");
      }
    }
  }

  /**
   * Generate a file with known word distribution
   */
  private static void generateFileWithKnownDistribution(String fileName) throws IOException {
    // Define words and their exact counts
    Map<String, Integer> wordDistribution = new HashMap<>();
    wordDistribution.put("the", 50);
    wordDistribution.put("mapreduce", 30);
    wordDistribution.put("data", 25);
    wordDistribution.put("processing", 20);
    wordDistribution.put("distributed", 15);
    wordDistribution.put("computing", 10);
    wordDistribution.put("parallel", 10);
    wordDistribution.put("algorithm", 8);
    wordDistribution.put("system", 8);
    wordDistribution.put("cluster", 7);
    wordDistribution.put("performance", 7);
    wordDistribution.put("hadoop", 6);
    wordDistribution.put("framework", 5);
    wordDistribution.put("job", 5);
    wordDistribution.put("task", 5);

    // Additional common words to make sentences flow
    List<String> fillerWords = Arrays.asList("and", "in", "of", "is", "for", "to", "with", "a", "an", "on", "by");

    StringBuilder content = new StringBuilder();

    // Create content with the exact frequency
    for (Map.Entry<String, Integer> entry : wordDistribution.entrySet()) {
      String word = entry.getKey();
      int count = entry.getValue();

      for (int i = 0; i < count; i++) {
        // Add some filler words occasionally for better readability
        if (random.nextDouble() < 0.3 && content.length() > 0) {
          String filler = fillerWords.get(random.nextInt(fillerWords.size()));
          content.append(filler).append(" ");
        }

        // Add the target word
        content.append(word).append(" ");

        // Add period occasionally
        if (random.nextDouble() < 0.15) {
          content.append(". ");

          // Capitalize next word if not at the end
          if (i < count - 1) {
            String nextWord = Character.toUpperCase(word.charAt(0)) + word.substring(1);
            content.append(nextWord).append(" ");
            i++; // Count this occurrence
          }
        }
      }
    }

    // Write content to file
    try (FileWriter writer = new FileWriter(new File(OUTPUT_DIR, fileName))) {
      writer.write(content.toString());
    }

    // Write expected word counts to a companion file
    try (FileWriter writer = new FileWriter(new File(OUTPUT_DIR, fileName + ".expected.csv"))) {
      writer.write("word,count\n");

      // Include the main distribution
      for (Map.Entry<String, Integer> entry : wordDistribution.entrySet()) {
        writer.write(entry.getKey() + "," + entry.getValue() + "\n");
      }

      // Note: This doesn't account for the filler words or capitalized versions
      // In a real implementation, you'd need to count these accurately as well
    }
  }
}