package com.mapreduce.web.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import com.mapreduce.web.util.DatabaseUtil;

@WebServlet("/test-db")
public class TestDBServlet extends HttpServlet {
    // protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    //     try {
    //         DatabaseUtil.initialize();
    //         resp.getWriter().println("Database pool initialized successfully!");
    //     } catch (Exception e) {
    //         resp.getWriter().println("FAILED: " + e.getMessage());
    //         e.printStackTrace();
    //     }
    // }

    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    try {
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection conn = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/mapreduce?useSSL=ture",
                "admin", "admin")) {
                
            resp.getWriter().println("Raw JDBC connection SUCCESS!");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1");
                rs.next();
                resp.getWriter().println("Database response: " + rs.getInt(1));
            }
        }
    } catch (Exception e) {
        resp.getWriter().println("Raw JDBC FAILED: " + e.getMessage());
        e.printStackTrace(new PrintWriter(resp.getWriter()));
    }
}
}