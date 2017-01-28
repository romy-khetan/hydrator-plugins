/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.aggregator;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchAggregatorContext;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import com.google.common.collect.Iterables;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.buffer.PriorityBuffer;

import java.sql.Struct;
import java.util.*;

/**
 *Sampling
 */
public class Sampling extends BatchAggregator<String, StructuredRecord, StructuredRecord>{

    private enum TYPE {
        SYSTEMATIC, RESERVOIR
    }

    private SamplingConfig config;

    public Sampling(SamplingConfig config) {
        this.config = config;
    }

    @Override
    public void prepareRun(BatchAggregatorContext context) throws Exception {
        context.setNumPartitions(1);
    }

    @Override
    public void initialize(BatchRuntimeContext context) throws Exception {

    }

    @Override
    public void groupBy(StructuredRecord record, Emitter<String> emitter) throws Exception {
        emitter.emit("sample");
    }

    @Override
    public void aggregate(String groupKey, Iterator<StructuredRecord> iterator,
                          Emitter<StructuredRecord> emitter) throws Exception {
        switch(TYPE.valueOf(config.samplingType.toUpperCase())) {
            case SYSTEMATIC:
                int finalSampleSize = config.sampleSize;
                if(config.overSamplingPercentage != null) {
                    finalSampleSize = Math.round(finalSampleSize +
                            (finalSampleSize * (config.overSamplingPercentage / 100)));
                }

                int sampleIndex = Math.round(config.totalRecords / finalSampleSize);
                Float random = new Float(0);
                if(config.random != null) {
                    random = config.random;
                } else {
                    random = new Random().nextFloat();
                }
                int firstSampleIndex = Math.round(sampleIndex * random);
                List<StructuredRecord> records = IteratorUtils.toList(iterator);
                int counter = 0;
                emitter.emit(records.get(firstSampleIndex));
                counter++;

                while(counter < finalSampleSize) {
                    int index = firstSampleIndex + (counter * sampleIndex);
                    emitter.emit(records.get(index - 1));
                    counter++;
                }
                break;

            case RESERVOIR:
                PriorityBuffer sampleData = new PriorityBuffer(true, new Comparator<StructuredRecord>() {
                    @Override
                    public int compare(StructuredRecord o1, StructuredRecord o2) {
                        if ((float) o1.get("random") < (float) o1.get("random")) {
                            return 1;
                        } else if((float) o1.get("random") > (float) o1.get("random")) {
                            return -1;
                        } else {
                            return 0;
                        }
                    }
                });

                Schema inputSchema = null;
                int count = 1;
                Random randomValue = new Random();
                while(count <= config.sampleSize) {
                    StructuredRecord record = iterator.next();
                    inputSchema = record.getSchema();
/*                    StructuredRecord.Builder builder =
                            StructuredRecord.builder(createSchemaWithRandomField(inputSchema));
                    for(Schema.Field field : record.getSchema().getFields()) {
                        builder.set(field.getName(), record.get(field.getName()));
                    }
                    builder.set("random", randomValue.nextFloat());*/
                    sampleData.add(getBuilder(record, randomValue.nextFloat(), createSchemaWithRandomField(inputSchema)));
                    //sampleData.add(builder.build());
                }

                while(iterator.hasNext()) {
                    StructuredRecord structuredRecord = (StructuredRecord) sampleData.get();
                    Float randomFloat = randomValue.nextFloat();
                    if((float) structuredRecord.get("random") < randomFloat) {
                        sampleData.remove();
                        StructuredRecord record = iterator.next();
                        /*StructuredRecord.Builder builder =
                                StructuredRecord.builder();
                        for(Schema.Field field : record.getSchema().getFields()) {
                            builder.set(field.getName(), record.get(field.getName()));
                        }
                        builder.set("random", randomFloat);*/
                        sampleData.add(getBuilder(record, randomFloat, structuredRecord.getSchema()));                    }
                }

                Iterator<StructuredRecord> sampleDataIterator = sampleData.iterator();
                while(sampleDataIterator.hasNext()) {
                    StructuredRecord sampledRecord = sampleDataIterator.next();
                    StructuredRecord.Builder builder =
                            StructuredRecord.builder(inputSchema);
                    for(Schema.Field field : sampledRecord.getSchema().getFields()) {
                        if(!field.getName().equalsIgnoreCase("random")) {
                            builder.set(field.getName(), sampledRecord.get(field.getName()));
                        }
                    }
                    emitter.emit(builder.build());
                }
        }

    }

    public StructuredRecord getBuilder(StructuredRecord record, Float random, Schema schema) {
        StructuredRecord.Builder builder =
                StructuredRecord.builder(schema);
        for(Schema.Field field : record.getSchema().getFields()) {
            builder.set(field.getName(), record.get(field.getName()));
        }
        builder.set("random", random);
        return builder.build();
    }

    /**
     * Builds the schema for Reservoir sampling algorithm. Adding field for random value.
     */
    private Schema createSchemaWithRandomField(Schema inputSchema) {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(Schema.Field.of("random", Schema.of(Schema.Type.FLOAT)));
        for (Schema.Field field : inputSchema.getFields()) {
            fields.add(field);
        }
        return Schema.recordOf("schema", fields);
    }

    /**
     * Config for Sampling Aggregator Plugin.
     */
    public class SamplingConfig extends AggregatorConfig {

        @Description("Name of the column in the input record which will be used to group the raw data. For Example, " +
                "id.")
        private Integer sampleSize;

        @Description("")
        private Float samplePercentage;

        @Description("")
        private Float overSamplingPercentage;

        @Description("")
        private Float random;

        @Description("")
        private String samplingType;

        @Description("")
        private Integer totalRecords;

    }

}
