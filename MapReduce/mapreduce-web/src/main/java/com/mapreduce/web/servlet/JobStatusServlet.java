package com.mapreduce.web.servlet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapreduce.web.dao.WordCountDAO;
import com.mapreduce.web.model.Job;
import com.mapreduce.web.model.WordCount;
import com.mapreduce.web.service.JobService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Servlet for retrieving job status and results
 */
@WebServlet(name = "JobStatusServlet", urlPatterns = {"/api/jobs/*"})
public class JobStatusServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LogManager.getLogger(JobStatusServlet.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Pattern JOB_ID_PATTERN = Pattern.compile("/api/jobs/([^/]+)(?:/.*)?");
    private static final Pattern WORDCOUNT_PATTERN = Pattern.compile("/api/jobs/([^/]+)/wordcount");
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");
        
        try {
            String pathInfo = req.getPathInfo();
            String requestURI = req.getRequestURI();
            
            // Get all jobs
            if (pathInfo == null || pathInfo.equals("/")) {
                getAllJobs(resp);
                return;
            }
            
            // Get word counts for a job
            Matcher wordcountMatcher = WORDCOUNT_PATTERN.matcher(requestURI);
            if (wordcountMatcher.find()) {
                String jobId = wordcountMatcher.group(1);
                getWordCounts(jobId, req, resp);
                return;
            }
            
            // Get single job status
            Matcher jobIdMatcher = JOB_ID_PATTERN.matcher(requestURI);
            if (jobIdMatcher.find()) {
                String jobId = jobIdMatcher.group(1);
                getJobStatus(jobId, resp);
                return;
            }
            
            // Invalid request path
            sendErrorResponse(resp, 400, "Invalid request path");
            
        } catch (Exception e) {
            logger.error("Error processing request", e);
            sendErrorResponse(resp, 500, "Error processing request: " + e.getMessage());
        }
    }
    
    /**
     * Get all jobs
     */
    private void getAllJobs(HttpServletResponse resp) throws IOException, SQLException {
        List<Job> jobs = JobService.getAllJobs();
        
        List<Map<String, Object>> jobsData = jobs.stream()
                .map(this::convertJobToMap)
                .collect(Collectors.toList());
        
        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("jobs", jobsData);
        
        objectMapper.writeValue(resp.getOutputStream(), response);
    }
    
    /**
     * Get job status
     */
    private void getJobStatus(String jobId, HttpServletResponse resp) throws IOException, SQLException {
        Job job = JobService.getJobStatus(jobId);
        
        if (job == null) {
            sendErrorResponse(resp, 404, "Job not found: " + jobId);
            return;
        }
        
        Map<String, Object> jobData = convertJobToMap(job);
        
        // Add word count summary for completed jobs
        if (job.getStatus() == Job.Status.COMPLETED) {
            try {
                int wordCount = WordCountDAO.getWordCountTotal(jobId);
                int totalFrequency = WordCountDAO.getTotalWordFrequency(jobId);
                
                jobData.put("uniqueWordCount", wordCount);
                jobData.put("totalWordFrequency", totalFrequency);
            } catch (Exception e) {
                logger.warn("Error getting word count summary for job {}: {}", jobId, e.getMessage());
            }
        }
        
        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("job", jobData);
        
        objectMapper.writeValue(resp.getOutputStream(), response);
    }
    
    /**
     * Get word counts for a job
     */
    private void getWordCounts(String jobId, HttpServletRequest req, HttpServletResponse resp) 
            throws IOException, SQLException {
        
        // Get limit parameter, default to 100
        int limit = 100;
        String limitParam = req.getParameter("limit");
        if (limitParam != null && !limitParam.isEmpty()) {
            try {
                limit = Integer.parseInt(limitParam);
            } catch (NumberFormatException e) {
                logger.warn("Invalid limit parameter: {}", limitParam);
            }
        }
        
        try {
            // Get job and verify it's completed
            Job job = JobService.getJobStatus(jobId);
            
            if (job == null) {
                sendErrorResponse(resp, 404, "Job not found: " + jobId);
                return;
            }
            
            if (job.getStatus() != Job.Status.COMPLETED) {
                sendErrorResponse(resp, 400, "Job not completed yet");
                return;
            }
            
            // Get word count results
            List<WordCount> wordCounts = JobService.getWordCounts(jobId, limit);
            
            Map<String, Integer> wordCountMap = wordCounts.stream()
                    .collect(Collectors.toMap(WordCount::getWord, WordCount::getCount));
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("jobId", jobId);
            response.put("wordCount", wordCountMap);
            response.put("count", wordCounts.size());
            response.put("totalWordCount", WordCountDAO.getTotalWordFrequency(jobId));
            
            objectMapper.writeValue(resp.getOutputStream(), response);
            
        } catch (IllegalArgumentException e) {
            sendErrorResponse(resp, 404, e.getMessage());
        } catch (IllegalStateException e) {
            sendErrorResponse(resp, 400, e.getMessage());
        }
    }
    
    /**
     * Convert Job object to Map for JSON serialization
     */
    private Map<String, Object> convertJobToMap(Job job) {
        Map<String, Object> map = new HashMap<>();
        map.put("jobId", job.getJobId());
        map.put("status", job.getStatus().name());
        map.put("inputFile", job.getInputFile());
        map.put("numMapTasks", job.getNumMapTasks());
        map.put("numReduceTasks", job.getNumReduceTasks());
        map.put("storageType", job.getStorageType().name());
        map.put("inputBlobUrl", job.getInputBlobUrl());
        map.put("outputBlobUrl", job.getOutputBlobUrl());
        map.put("createdTime", job.getCreatedTime() != null ? job.getCreatedTime().toString() : null);
        map.put("startTime", job.getStartTime() != null ? job.getStartTime().toString() : null);
        map.put("finishTime", job.getFinishTime() != null ? job.getFinishTime().toString() : null);
        return map;
    }
    
    /**
     * Send error response
     */
    private void sendErrorResponse(HttpServletResponse resp, int statusCode, String message) throws IOException {
        resp.setStatus(statusCode);
        
        Map<String, Object> response = new HashMap<>();
        response.put("status", "error");
        response.put("message", message);
        
        objectMapper.writeValue(resp.getOutputStream(), response);
    }
}