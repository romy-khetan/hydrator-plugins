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

package co.cask.hydrator.plugin.transform;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Validator;
import co.cask.cdap.etl.mock.common.MockEmitter;
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import co.cask.cdap.etl.mock.transform.MockTransformContext;
import co.cask.hydrator.plugin.validator.CoreValidator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

/**
 * Validator transformation testing
 */
public class ValidatorTransformTest {

  private static final Schema SCHEMA = Schema.recordOf("validator",
                                                       Schema.Field.of("date", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("url", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("content_length", Schema.of(Schema.Type.INT)));

  @Test
  public void testValidatorTransformWithMap() throws Exception {
    ValidatorTransform.ValidatorConfig config = new ValidatorTransform.ValidatorConfig();
    config.validationScript =
      "   function isValid(input, context) { " +
        "      var isValid = true; " +
        "      var errMsg = \"\";" +
        "      var errCode = 0;" +
        "      var coreValidator = context.getValidator(\"coreValidator\");" +
        "      if (!coreValidator.isDate(input.date)) { " +
        "         isValid = false; errMsg = input.date + \"is invalid date\"; errCode = 5;" +
        "      } else if (!coreValidator.isUrl(input.url)) { " +
        "         isValid = false; errMsg = \"invalid url\"; errCode = 7;" +
        "      } else if (!coreValidator.isInRange(input.content_length, 0, 1024 * 1024)) {" +
        "         isValid = false; errMsg = \"content length >1MB\"; errCode = 10;" +
        "      }" +
        "      context.getMetrics().count(\"total.processed\", 1);" +
        "      context.getMetrics().pipelineCount(\"total.processed\", 1);" +
        "      context.getLogger().info(\"Test Log from Validator Transform\");" +
        "      return {'isValid': isValid, 'errorCode': errCode, 'errorMsg': errMsg}; " +
        "   };";

    config.validators = "core";

    ValidatorTransform transform = new ValidatorTransform(config);
    MockTransformContext mockContext = new MockTransformContext("validator.1");
    transform.setUpInitialScript(mockContext, ImmutableList.<Validator>of(new CoreValidator()));
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    StructuredRecord validRecord = StructuredRecord.builder(SCHEMA)
      .set("date", "1/2/1988")
      .set("url", "http://xyz.com")
      .set("content_length", 120)
      .build();

    StructuredRecord invalidRecord1 = StructuredRecord.builder(SCHEMA)
      .set("date", "1/2-1988")
      .set("url", "http://xyz.com")
      .set("content_length", 120)
      .build();

    StructuredRecord invalidRecord2 = StructuredRecord.builder(SCHEMA)
      .set("date", "1/2/1988") // invalid-date
      .set("url", "xyz.com") // invalid url (missing protocol)
      .set("content_length", 120)
      .build();

    StructuredRecord invalidRecord3 = StructuredRecord.builder(SCHEMA)
      .set("date", "1/2-1988")
      .set("url", "http://xyz.com")
      .set("content_length", 1025 * 1024) // invalid content_length > 1MB
      .build();

    transform.transform(validRecord, emitter);
    transform.transform(invalidRecord1, emitter);
    transform.transform(invalidRecord2, emitter);
    transform.transform(invalidRecord3, emitter);

    Assert.assertEquals(1, emitter.getEmitted().size());
    Assert.assertEquals(3, emitter.getErrors().size());
    Assert.assertEquals(4, mockContext.getMockMetrics().getCount("total.processed"));
    Assert.assertEquals(4, mockContext.getMockMetrics().getPipelineCount("validator.1.total.processed"));
  }

  @Test
  public void testSchemaValidation() throws Exception {
    ValidatorTransform.ValidatorConfig config = new ValidatorTransform.ValidatorConfig();
    config.validationScript =
      "   function isValid(input, context) { " +
        "      var isValid = true; " +
        "      var errMsg = \"\";" +
        "      var errCode = 0;" +
        "      var coreValidator = context.getValidator(\"coreValidator\");" +
        "      if (!coreValidator.isDate(input.date)) { " +
        "         isValid = false; errMsg = input.date + \"is invalid date\"; errCode = 5;" +
        "      } else if (!coreValidator.isUrl(input.url)) { " +
        "         isValid = false; errMsg = \"invalid url\"; errCode = 7;" +
        "      } else if (!coreValidator.isInRange(input.content_length, 0, 1024 * 1024)) {" +
        "         isValid = false; errMsg = \"content length >1MB\"; errCode = 10;" +
        "      }" +
        "      context.getMetrics().count(\"total.processed\", 1);" +
        "      context.getMetrics().pipelineCount(\"total.processed\", 1);" +
        "      context.getLogger().info(\"Test Log from Validator Transform\");" +
        "      return {'isValid': isValid, 'errorCode': errCode, 'errorMsg': errMsg}; " +
        "   };";

    config.validators = "core";

    ValidatorTransform transform = new ValidatorTransform(config);
    Schema outputSchema = Schema.recordOf(
      "smallerSchema",
      Schema.Field.of("x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("y", Schema.of(Schema.Type.LONG)));

    MockPipelineConfigurer pipelineConfigurer = new MockPipelineConfigurer(outputSchema,
                                                                           ImmutableMap.<String, Object>of(
                                                                             CoreValidator.ID, new CoreValidator()));
    transform.configurePipeline(pipelineConfigurer);
    Assert.assertEquals(outputSchema, pipelineConfigurer.getOutputSchema());
  }
}
