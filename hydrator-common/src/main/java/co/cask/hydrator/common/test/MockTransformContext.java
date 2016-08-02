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

package co.cask.hydrator.common.test;

import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.TransformContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Mock context for unit tests
 */
public class MockTransformContext implements TransformContext {
  private final PluginProperties pluginProperties;
  private final MockStageMetrics metrics;
  private final LookupProvider lookup;
  private final String stageName;

  public MockTransformContext() {
    this("someStage");
  }

  public MockTransformContext(String stageName) {
    this(stageName, new HashMap<String, String>());
  }

  public MockTransformContext(String stageName, Map<String, String> args) {
    this(stageName, args, new MockLookupProvider(null));
  }

  public MockTransformContext(String stageName, Map<String, String> args, LookupProvider lookup) {
    this.pluginProperties = PluginProperties.builder().addAll(args).build();
    this.lookup = lookup;
    this.metrics = new MockStageMetrics(stageName);
    this.stageName = stageName;
  }

  @Override
  public PluginProperties getPluginProperties() {
    return pluginProperties;
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return null;
  }

  @Override
  public StageMetrics getMetrics() {
    return metrics;
  }

  public MockStageMetrics getMockMetrics() {
    return metrics;
  }

  @Override
  public String getStageName() {
    return stageName;
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return null;
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return null;
  }

  @Override
  public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
    return lookup.provide(table, arguments);
  }
}
