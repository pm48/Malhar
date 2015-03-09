/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.smtp;

import com.datatorrent.api.*;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.mail.Message;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetup;
import com.icegreen.greenmail.util.ServerSetupTest;
import java.io.File;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.junit.*;
import org.junit.rules.TestWatcher;

public class SmtpOutputOperatorTest
{

  String subject = "ALERT!";
  String content = "This is an SMTP operator test {}";
  String from = "jenkins@datatorrent.com";
  String to = "jenkins@datatorrent.com";
  String cc = "jenkinsCC@datatorrent.com";
  GreenMail greenMail = null;
  SmtpOutputOperator node;

  Map<String, String> data;
  public static class TestMeta extends TestWatcher
  {
    public String dir = null;
    Context.OperatorContext context;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;
      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, "FileInputOperatorTest");
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
    }
  }

  @Rule public TestMeta testMeta = new TestMeta();
  @Before
  public void setup() throws Exception
  {
    node = new SmtpOutputOperator();
    greenMail = new GreenMail(ServerSetupTest.ALL);
    greenMail.start();
    node.setFrom(from);
    node.setContent(content);
    node.setSubject("hello");
    node.setSmtpHost("127.0.0.1");
    node.setSmtpPort(ServerSetupTest.getPortOffset() + ServerSetup.SMTP.getPort());
    node.setSmtpUserName(from);
    node.setSmtpPassword("<password>");
    //node.setUseSsl(true);
    node.setSubject(subject);
    data = new HashMap<String, String>();
    data.put("alertkey", "alertvalue");

  }

  @After
  public void teardown() throws Exception
  {
    node.teardown();
    greenMail.stop();
    Thread.sleep(1000);
  }

  @Test
  public void testSmtpOutputNode() throws Exception
  {
    Map<String, String> recipients = Maps.newHashMap();
    recipients.put("to", to + "," + cc);
    recipients.put("cc", cc);
    node.setRecipients(recipients);
    node.setSubject("hello");
    node.setup(null);
    node.beginWindow(1000);
    String data = "First test message";
    node.input.process(data);
    node.endWindow();
    Assert.assertTrue(greenMail.waitForIncomingEmail(5000, 1));
    MimeMessage[] messages = greenMail.getReceivedMessages();
    Assert.assertEquals(3, messages.length);
    String receivedContent = messages[0].getContent().toString().trim();
    String expectedContent = content.replace("{}", data.toString()).trim();

    Assert.assertTrue(expectedContent.equals(receivedContent));
    Assert.assertEquals(from, ((InternetAddress) messages[0].getFrom()[0]).getAddress());
    Assert.assertEquals(to, messages[0].getRecipients(Message.RecipientType.TO)[0].toString());
    Assert.assertEquals(cc, messages[0].getRecipients(Message.RecipientType.TO)[1].toString());
    Assert.assertEquals(cc, messages[0].getRecipients(Message.RecipientType.CC)[0].toString());

  }

  @Test
  public void test() throws Exception
  {
    Map<String, String> recipients = Maps.newHashMap();
    recipients.put("to", to);
    node.setRecipients(recipients);
    node.setup(null);
    node.beginWindow(1000);
    node.input.process(data);
    node.endWindow();
    Assert.assertTrue(greenMail.waitForIncomingEmail(5000, 1));
    MimeMessage[] messages = greenMail.getReceivedMessages();
    Assert.assertEquals(1, messages.length);
    String receivedContent = messages[0].getContent().toString().trim();
    String expectedContent = content.replace("{}", data.toString()).trim();

    Assert.assertTrue(expectedContent.equals(receivedContent));
    Assert.assertEquals(from, ((InternetAddress) messages[0].getFrom()[0]).getAddress());
    Assert.assertEquals(to, messages[0].getAllRecipients()[0].toString());
  }

  @Test
  public void testProperties() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.subject", subject);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.content", content);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.from", from);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.smtpHost", "127.0.0.1");
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.smtpUserName", from);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.smtpPassword", "<password>");
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.recipients.TO", to + "," + cc);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.recipients.CC", cc);

    final AtomicReference<SmtpOutputOperator> o1 = new AtomicReference<SmtpOutputOperator>();
    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        o1.set(dag.addOperator("o1", new SmtpOutputOperator()));
      }
    };

    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(app, conf);
    Assert.assertEquals("checking TO list", to + "," + cc, o1.get().getRecipients().get("TO"));
    Assert.assertEquals("checking CC list", cc, o1.get().getRecipients().get("CC"));

  }

  @Test
  public void testIdempotency() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    Map<String, String> recipients = Maps.newHashMap();
    recipients.put("to", to + "," + cc);
    recipients.put("cc", cc);
    node.setRecipients(recipients);
    node.setSubject("hello");
    node.setup(null);
    node.beginWindow(1000);
    String data = "First test message";
    node.input.process(data);
    node.endWindow();
    Assert.assertTrue(greenMail.waitForIncomingEmail(5000, 1));
    MimeMessage[] messages = greenMail.getReceivedMessages();
    Assert.assertEquals(3, messages.length);
    String receivedContent = messages[0].getContent().toString().trim();
    String expectedContent = content.replace("{}", data.toString()).trim();

    Assert.assertTrue(expectedContent.equals(receivedContent));
    Assert.assertEquals(from, ((InternetAddress) messages[0].getFrom()[0]).getAddress());
    Assert.assertEquals(to, messages[0].getRecipients(Message.RecipientType.TO)[0].toString());
    Assert.assertEquals(cc, messages[0].getRecipients(Message.RecipientType.TO)[1].toString());
    Assert.assertEquals(cc, messages[0].getRecipients(Message.RecipientType.CC)[0].toString());
    List<String> allLines = Lists.newArrayList();
    for (int file = 0; file < 2; file++) {
      List<String> lines = Lists.newArrayList();
      for (int line = 0; line < 2; line++) {
        lines.add("f" + file + "l" + line);
      }
      allLines.addAll(lines);
      FileUtils.write(new File(testMeta.dir, "file" + file), StringUtils.join(lines, '\n'));
    }

    SmtpOutputOperator oper = new SmtpOutputOperator();
    IdempotentStorageManager.FSIdempotentStorageManager manager = new IdempotentStorageManager.FSIdempotentStorageManager();
    manager.setRecoveryPath(testMeta.dir + "/recovery");

    oper.setIdempotentStorageManager(manager);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;



    oper.setup(testMeta.context);
    for (long wid = 0; wid < 3; wid++) {
      oper.beginWindow(wid);
      oper.input.process(data);
      oper.endWindow();
    }
    oper.teardown();

    sink.clear();

    //idempotency  part
    oper.setup(testMeta.context);
    for (long wid = 0; wid < 3; wid++) {
      oper.beginWindow(wid);
      oper.endWindow();
    }
    Assert.assertEquals("number tuples", 4, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", allLines, queryResults.collectedTuples);
    oper.teardown();
  }










}
