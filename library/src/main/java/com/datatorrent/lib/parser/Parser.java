/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.parser;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.DTThrowable;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

public class Parser extends AbstractParser<Map<String, Object>>
{
  protected transient ICsvMapReader csvReader = null;
  /**
   * The output is a map with key being the field name and value being the value of the field.
   */
  public final transient DefaultOutputPort<Map<String, Object>> output = new DefaultOutputPort<Map<String, Object>>();

  @Override
  public ICsvMapReader getReader(BufferedReader br, CsvPreference preference)
  {
    csvReader = new CsvMapReader(br, preference);
    return csvReader;

  }

  @Override
  public Map<String, Object> readData(String[] properties, CellProcessor[] processors)
  {
    Map<String, Object> fieldValueMapping = null;
    try {
      fieldValueMapping = csvReader.read(properties, processors);
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    return fieldValueMapping;

  }

}
