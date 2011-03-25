package org.mozilla.grouper.worker;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("serial")
public class Updater extends HttpServlet {

    static final Logger LOG = LoggerFactory.getLogger(Updater.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
        response.setStatus(405);
        response.setHeader("Allow", "POST");
        response.getWriter().write("Use POST method to trigger a cluster update.");
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
    throws ServletException, IOException {
        response.setStatus(200);
        response.getWriter().write("Test");

    }
}
