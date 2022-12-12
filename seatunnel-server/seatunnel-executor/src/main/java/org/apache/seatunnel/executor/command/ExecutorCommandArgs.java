/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.executor.command;

import com.beust.jcommander.Parameter;

public class ExecutorCommandArgs {
    @Parameter(names = {"-host", "--host"},
        description = "SeaTunnel Web Service Host")
    private String host;

    @Parameter(names = {"-j", "--job-id"},
        description = "Get job status by JobId")
    private String jobId;

    @Parameter(names = {"-p", "--project-code"},
        description = "The project code job belonged to")
    private String projectCode;

    @Parameter(names = {"-t", "--token"},
        description = "The token of the job")
    private String token;

    @Parameter(names = {"--help"},
        help = true,
        description = "Show the usage message")
    private boolean help = false;

    public String getHost() {
        return host;
    }

    public String getJobId() {
        return jobId;
    }

    public boolean isHelp() {
        return help;
    }

    public String getProjectCode() {
        return projectCode;
    }

    public String getToken() {
        return token;
    }
}
