/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Assert;

/**
 * Functional test for {@link com.datatorrent.lib.io.HttpPostOutputOperator}.
 */
public class DMSOperatorTest
{
  boolean receivedMessage = false;

  @Test
  public void testHttpOutputNode() throws Exception
  {

    final List<String> receivedMessages = new ArrayList<String>();
    Handler handler = new AbstractHandler()
    {
      @Override
      @Consumes({MediaType.APPLICATION_JSON})
      public void handle(String string, Request rq, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException
      {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy(request.getInputStream(), bos);
        receivedMessages.add(new String(bos.toByteArray()));
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>Thanks</h1>");
        ((Request)request).setHandled(true);
        receivedMessage = true;
      }

    };

    Server server = new Server(0);
    server.setHandler(handler);
    server.start();

    String url = "http://localhost:" + server.getConnectors()[0].getLocalPort() + "/somecontext";
    System.out.println("url: " + url);


    DMSPostOperator<Object> node = new DMSPostOperator<Object>();
    node.setUrl(url);

    node.setup(null);

    // Wait till the message is received or a maximum timeout elapses
    int timeoutMillis = 10000;
    while (!receivedMessage && timeoutMillis > 0) {
      timeoutMillis -= 20;
      Thread.sleep(20);
    }

    Assert.assertEquals("number requests", 1, receivedMessages.size());
    System.out.println(receivedMessages.get(0));


    receivedMessages.clear();
    String stringData = "stringData";
    node.input.process(stringData);
//    Assert.assertEquals("number requests", 1, receivedMessages.size());
//    Assert.assertEquals("request body " + receivedMessages.get(0), stringData, receivedMessages.get(0));


    node.teardown();
    server.stop();
  }

}
