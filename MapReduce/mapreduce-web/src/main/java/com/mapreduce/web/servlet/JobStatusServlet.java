package com.mapreduce.web.servlet;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    private static final Pattern EXECUTION_TIME_PATTERN = Pattern.compile("/api/jobs/([^/]+)/execution-time");

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
                getWordCounts(jobId, resp);
                return;
            }

            // Get job execution time
            Matcher executionTimeMatcher = EXECUTION_TIME_PATTERN.matcher(requestURI);
            if (executionTimeMatcher.find()) {
                String jobId = executionTimeMatcher.group(1);
                getJobExecutionTime(jobId, resp);
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

        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("jobs", jobs);

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

        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("job", job);

        objectMapper.writeValue(resp.getOutputStream(), response);
    }

    /**
     * Get word counts for a job
     */
    private void getWordCounts(String jobId, HttpServletResponse resp) throws IOException, SQLException {
        try {
            // Check if job exists and is completed
            Job job = JobService.getJobStatus(jobId);

            if (job == null) {
                sendErrorResponse(resp, 404, "Job not found: " + jobId);
                return;
            }

            if (job.getStatus() != Job.Status.COMPLETED) {
                sendErrorResponse(resp, 400, "Job is not completed yet");
                return;
            }

            // Get limit parameter (default to 100)
            int limit = 170;

            // Get word counts
            List<WordCount> wordCounts = JobService.getWordCounts(jobId, limit);
            int totalUniqueWords = JobService.getUniqueWordCount(jobId);
            int totalWordFrequency = JobService.getTotalWordFrequency(jobId);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("jobId", jobId);
            response.put("wordCounts", wordCounts);
            response.put("totalUniqueWords", totalUniqueWords);
            response.put("totalWordFrequency", totalWordFrequency);
            response.put("limit", limit);

            objectMapper.writeValue(resp.getOutputStream(), response);
        } catch (IllegalStateException e) {
            sendErrorResponse(resp, 400, e.getMessage());
        } catch (IllegalArgumentException e) {
            sendErrorResponse(resp, 404, e.getMessage());
        }
    }

    /**
     * Get job execution time
     */
    private void getJobExecutionTime(String jobId, HttpServletResponse resp) throws IOException, SQLException {
        // Get job details - reuses existing connection from JobService
        Job job = JobService.getJobStatus(jobId);

        if (job == null) {
            sendErrorResponse(resp, 404, "Job not found: " + jobId);
            return;
        }

        Map<String, Object> response = new HashMap<>();
        response.put("jobId", job.getJobId());
        response.put("status", job.getStatus().name());

        // Calculate execution time if job is completed
        if (job.getStatus() == Job.Status.COMPLETED || job.getStatus() == Job.Status.FAILED) {
            if (job.getStartTime() != null && job.getFinishTime() != null) {
                long executionTimeMs = job.getFinishTime().getTime() - job.getStartTime().getTime();

                // Format execution time
                long seconds = executionTimeMs / 1000;
                long minutes = seconds / 60;
                long hours = minutes / 60;

                seconds = seconds % 60;
                minutes = minutes % 60;

                String formattedTime = String.format("%02d:%02d:%02d", hours, minutes, seconds);

                response.put("executionTime", formattedTime);
                response.put("executionTimeMs", executionTimeMs);
                response.put("startTime", job.getStartTime().toString());
                response.put("finishTime", job.getFinishTime().toString());
            } else {
                response.put("executionTime", "Unknown");
                response.put("reason", "Missing start or finish time");
            }
        } else {
            response.put("executionTime", "In progress");
            if (job.getStartTime() != null) {
                long elapsedTimeMs = System.currentTimeMillis() - job.getStartTime().getTime();

                // Format elapsed time
                long seconds = elapsedTimeMs / 1000;
                long minutes = seconds / 60;
                long hours = minutes / 60;

                seconds = seconds % 60;
                minutes = minutes % 60;

                String formattedTime = String.format("%02d:%02d:%02d (so far)", hours, minutes, seconds);

                response.put("elapsedTime", formattedTime);
                response.put("elapsedTimeMs", elapsedTimeMs);
                response.put("startTime", job.getStartTime().toString());
            }
        }

        objectMapper.writeValue(resp.getOutputStream(), response);
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