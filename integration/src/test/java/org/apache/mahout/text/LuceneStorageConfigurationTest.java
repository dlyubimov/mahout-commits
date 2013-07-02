/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.mahout.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LuceneStorageConfigurationTest {
  
  @Test
  public void testSerialization() throws Exception {
    Configuration configuration = new Configuration();
    Path indexPath = new Path("indexPath");
    Path outputPath = new Path("outputPath");
    LuceneStorageConfiguration luceneStorageConfiguration =
      new LuceneStorageConfiguration(configuration, asList(indexPath), outputPath, "id", asList("field"));

    Configuration serializedConfiguration = luceneStorageConfiguration.serialize();

    LuceneStorageConfiguration deSerializedConfiguration = new LuceneStorageConfiguration(serializedConfiguration);

    assertEquals(luceneStorageConfiguration, deSerializedConfiguration);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testSerializationNotSerialized() throws IOException {
    new LuceneStorageConfiguration(new Configuration());
  }
}
