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

package co.cask.hydrator.plugin.batch.action;

/**
 *Exception class to handle SSH authentication exceptions
 */
public class SSHAuthenticationException extends Exception {

  private String user;
  private String host;
  private Integer port;

  public SSHAuthenticationException(String message, String user, String host, Integer port) {
    super(message);
    this.user = user;
    this.host = host;
    this.port = port;
  }

  @Override
  public String getMessage() {
    return String.format(super.getMessage(), user, host, port);
  }
}
