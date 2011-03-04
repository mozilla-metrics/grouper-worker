package org.mozilla.grouper.clusterd;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


@SuppressWarnings("serial")
public class Updater extends HttpServlet {
    
    static final Logger LOG = LoggerFactory.getLogger(Updater.class);
    
    protected void doGet(HttpServletRequest request, HttpServletResponse response) 
    throws ServletException, IOException {
        response.setStatus(405);
        response.setHeader("Allow", "POST");
        response.getWriter().write("Use POST method to trigger a cluster update.");
    }
    
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
    throws ServletException, IOException {
        response.setStatus(200);
        response.getWriter().write("Test");
        
    }
}
