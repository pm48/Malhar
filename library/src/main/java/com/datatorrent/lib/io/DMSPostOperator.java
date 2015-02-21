/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.io;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.DTThrowable;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DMSPostOperator<T> extends AbstractHttpOperator<T>
{
  protected transient WebResource resource;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    JSONObject requestJSON = buildJSON();
    resource = wsClient.resource(url);
    logger.debug("URL: {}", url);
    ClientResponse response = resource.accept("application/json").type("application/json").post(ClientResponse.class, requestJSON);
    // check response status code
        if (response.getStatus() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatus());
        }

    // display response
    String output = response.getEntity(String.class);
    logger.debug("response is {}", output + "\n");
  }

  @Override
  protected void processTuple(T t)
  {
  }

  private JSONObject buildJSON()
  {
    JSONObject jsonObj = new JSONObject();
    String jsonInput = "{\n"
            + " \"header\": {\n"
            + " \"clientName\": \"it\",\n"
            + " \"version\": 1\n"
            + " },\n"
            + " \"operations\": [\n"
            + " {\n"
            + " \"operation\": \"UPSERT\",\n"
            + " \"docType\": \"device\",\n"
            + " \"data\": [\n"
            + " {\n"
            + " \"device.naturalKey\": \"011350FFFE0020FE\",\n"
            + " \"device.deviceType\": \"ACCESS_POINT\",\n"
            + " \"device.name\": null,\n"
            + " \"location.height\": 5.6,\n"
            + " \"device.isLockedState\": true,\n"
            + " \"location.latLonPoint\":\n"
            + " }\n"
            + " ]\n"
            + " }\n"
            + " ]\n"
            + "}";
    try {
      JSONObject headerValue = new JSONObject();
      headerValue.put("clientName", "it");
      headerValue.put("version", "1");
      jsonObj.put("header", headerValue);
      JSONObject operationValues = new JSONObject();
      operationValues.put("operation", "UPSERT");
      operationValues.put("docType", "device");
      JSONArray operationArray = new JSONArray();
      operationArray.put(operationValues);
      jsonObj.put("operations", operationArray);
    }
    catch (JSONException ex) {
      DTThrowable.rethrow(ex);
    }
    return jsonObj;
  }

  private static final Logger logger = LoggerFactory.getLogger(DMSPostOperator.class);

}
