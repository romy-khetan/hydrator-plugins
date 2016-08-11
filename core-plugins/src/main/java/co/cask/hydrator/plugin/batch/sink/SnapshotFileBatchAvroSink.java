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

package co.cask.hydrator.plugin.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.hydrator.plugin.common.SnapshotFileSetConfig;
import co.cask.hydrator.plugin.common.StructuredToAvroTransformer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;

import javax.annotation.Nullable;


/**
 * {@link SnapshotFileBatchSink} that stores data in Avro format.
 */
@Plugin(type = "batchsink")
@Name("SnapshotAvro")
@Description("Sink for a SnapshotFileSet that writes data in Avro format.")
public class SnapshotFileBatchAvroSink extends SnapshotFileBatchSink<AvroKey<GenericRecord>, NullWritable> {
  private StructuredToAvroTransformer recordTransformer;
  private final SnapshotAvroConfig config;

  public SnapshotFileBatchAvroSink(SnapshotAvroConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.schema);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(new AvroKey<>(recordTransformer.transform(input)), NullWritable.get()));
  }

  @Override
  protected void addFileProperties(FileSetProperties.Builder propertiesBuilder) {
    // parse it to make sure its valid
    try {
      new Schema.Parser().parse(config.schema);
    } catch (SchemaParseException e) {
      throw new IllegalArgumentException("Could not parse schema: " + e.getMessage(), e);
    }
    propertiesBuilder
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setOutputProperty("avro.schema.output.key", config.schema)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", config.schema)
      .add(DatasetProperties.SCHEMA, config.schema);
  }

  /**
   * Config for SnapshotFileBatchAvroSink
   */
  public static class SnapshotAvroConfig extends SnapshotFileSetConfig {
    @Description("The Avro schema of the record being written to the Sink as a JSON Object.")
    private String schema;

    public SnapshotAvroConfig(String name, @Nullable String basePath, String schema) {
      super(name, basePath, null, null);
      this.schema = schema;
    }
  }
}
