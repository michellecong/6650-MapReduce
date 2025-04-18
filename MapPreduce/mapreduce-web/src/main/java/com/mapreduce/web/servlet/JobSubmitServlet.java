package com.mapreduce.web.servlet;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
// import jakarta.servlet.ServletException;
// import jakarta.servlet.annotation.WebServlet;
// import jakarta.servlet.http.HttpServlet;
// import jakarta.servlet.http.HttpServletRequest;
// import jakarta.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapreduce.web.service.JobService;
import com.mapreduce.web.util.DatabaseUtil;

/**
 * Servlet for submitting new MapReduce jobs
 */
@WebServlet(name = "JobSubmitServlet", urlPatterns = {"/api/jobs"})
public class JobSubmitServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LogManager.getLogger(JobSubmitServlet.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");
        
        try {
            if (!DatabaseUtil.isInitialized()) {
                DatabaseUtil.initialize();
                logger.info("Database initialized on demand");
            }
        } catch (Exception e) {
            logger.error("Failed to initialize database connection3", e);
            sendErrorResponse(resp, 500, "Database initialization failed3: " + e.getMessage());
            return;
        }
        
        try {
            // Parse JSON request
            String requestBody = IOUtils.toString(req.getInputStream(), StandardCharsets.UTF_8);
            Map<String, Object> requestData = objectMapper.readValue(requestBody, Map.class);
            
            // Get text content from request
            String textContent = (String) requestData.get("text");
            if (textContent == null || textContent.trim().isEmpty()) {
                sendErrorResponse(resp, 400, "Text content is required");
                return;
            }
            
            // Get optional parameters
            String fileName = (String) requestData.getOrDefault("fileName", "input.txt");
            Integer numReduceTasks = requestData.containsKey("numReduceTasks") ? 
                    (Integer) requestData.get("numReduceTasks") : 5;
            Boolean useBlob = requestData.containsKey("useBlob") ? 
                    (Boolean) requestData.get("useBlob") : true;
            
            // Submit job
            String jobId = JobService.submitJob(textContent, fileName, numReduceTasks, useBlob);
            
            // Send success response
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Job submitted successfully");
            response.put("jobId", jobId);
            
            objectMapper.writeValue(resp.getOutputStream(), response);
            
        } catch (Exception e) {
            logger.error("Error processing job submission", e);
            sendErrorResponse(resp, 500, "Error processing job submission: " + e.getMessage());
        }
    }
    
    private void sendErrorResponse(HttpServletResponse resp, int statusCode, String message) throws IOException {
        resp.setStatus(statusCode);
        
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("status", "error");
        errorResponse.put("message", message);
        
        objectMapper.writeValue(resp.getOutputStream(), errorResponse);
    }
}