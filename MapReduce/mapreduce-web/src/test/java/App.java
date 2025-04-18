
import java.io.IOException;
import java.sql.SQLException;
import com.mapreduce.web.service.JobService;
import java.util.concurrent.TimeoutException;

public class App {
    public static void main(String[] args) {
        try {
            // Get text content from request
            String textContent = "Hello, world! This is a test input for MapReduce job.";
            
            // Get optional parameters
            String fileName = "input2.txt";
            Integer numReduceTasks = 5;
            Boolean useBlob = true;

            String jobId = JobService.submitJob(textContent, fileName, numReduceTasks, useBlob);
            System.out.println("Submitted job ID: " + jobId);
        } catch (IOException | SQLException | TimeoutException e) {
            e.printStackTrace();
        }
    }
}

