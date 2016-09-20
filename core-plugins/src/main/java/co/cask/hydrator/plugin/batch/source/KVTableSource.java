/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.hydrator.plugin.common.BatchReadableWritableConfig;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * CDAP Key Value Table Dataset Batch Source.
 */
@Plugin(type = "batchsource")
@Name("KVTable")
@Description("Reads the entire contents of a KeyValueTable. Outputs records with a 'key' field and a 'value' field. " +
  "Both fields are of type bytes.")
public class KVTableSource extends BatchReadableSource<byte[], byte[], StructuredRecord> {
  private static final Schema SCHEMA = Schema.recordOf(
    "keyValue",
    Schema.Field.of("key", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("value", Schema.of(Schema.Type.BYTES))
  );

  /**
   * Config class for KVTableSource
   */
  public static class KVTableConfig extends BatchReadableWritableConfig {

    public KVTableConfig(String name) {
      super(name);
    }
  }

  private final KVTableConfig kvTableConfig;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(SCHEMA);
  }

  public KVTableSource(KVTableConfig kvTableConfig) {
    super(kvTableConfig);
    this.kvTableConfig = kvTableConfig;
  }

  @Override
  protected Map<String, String> getProperties() {
    Map<String, String> properties = Maps.newHashMap(kvTableConfig.getProperties().getProperties());
    properties.put(Properties.BatchReadableWritable.NAME, kvTableConfig.getName());
    properties.put(Properties.BatchReadableWritable.TYPE, KeyValueTable.class.getName());
    return properties;
  }

  @Override
  public void transform(KeyValue<byte[], byte[]> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(StructuredRecord.builder(SCHEMA).set("key", input.getKey()).set("value", input.getValue()).build());
  }
}
