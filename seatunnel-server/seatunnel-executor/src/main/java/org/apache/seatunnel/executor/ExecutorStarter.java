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

package org.apache.seatunnel.executor;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.executor.command.ExecutorCommandArgs;
import org.apache.seatunnel.executor.job.JobInstance;
import org.apache.seatunnel.executor.util.CommandLineUtils;
import org.apache.seatunnel.executor.util.HttpClientProvider;
import org.apache.seatunnel.executor.util.HttpParameter;
import org.apache.seatunnel.executor.util.HttpResponse;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
public class ExecutorStarter {
    private static final String TMP_JOB_CONFIG_DIR = "/tmp/job";
    private final HttpParameter httpParameter = new HttpParameter();
    private final HttpClientProvider httpClientProvider = new HttpClientProvider(httpParameter);
    private final ExecutorCommandArgs executorCommandArgs;

    public ExecutorStarter(ExecutorCommandArgs executorCommandArgs) {
        this.executorCommandArgs = executorCommandArgs;
    }

    public void start() {
        // Get job instance from SeaTunnel Web Service
        JobInstance jobInstance = getJobInstance();
        String jobInstanceId = jobInstance.getJobInstanceId();
        // Write job temp config file
        String jobConfigFile = String.join(File.separator, new String[]{TMP_JOB_CONFIG_DIR, jobInstanceId});
        try (FileOutputStream fileOutputStream = new FileOutputStream(jobConfigFile)) {
            fileOutputStream.write(jobInstance.getJobConfig().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            String errorMsg = String.format("Generate temp job config file [%s] failed", jobConfigFile);
            throw new RuntimeException(errorMsg, e);
        }
        // Based on different engine to join different command
        JobInstance.EngineType engineType = JobInstance.EngineType.valueOf(jobInstance.getEngine().toUpperCase());
        String seaTunnelHomePath = getSeaTunnelHomePath();
        String execCommand;
        switch (engineType) {
            case FLINK:
                String flinkShell = String.join(File.separator,
                        new String[]{seaTunnelHomePath, "bin", CommandLineUtils.FLINK_STARTER_SHELL_NAME});
                execCommand = String.format("%s --config %s", flinkShell, jobConfigFile);
                break;
            case SPARK:
                String sparkShell = String.join(File.separator,
                        new String[]{seaTunnelHomePath, "bin", CommandLineUtils.SPARK_STARTER_SHELL_NAME});
                execCommand = String.format("%s --config %s -m %s -e %s", sparkShell, jobConfigFile,
                        jobInstance.getMaster(), jobInstance.getDeployMode());
                break;
            case SEATUNNEL:
                String seaTunnelShell = String.join(File.separator,
                        new String[]{seaTunnelHomePath, "bin", CommandLineUtils.SEATUNNEL_STARTER_SHELL_NAME});
                execCommand = String.format("%s --config %s", seaTunnelShell, jobConfigFile);
                break;
            default:
                throw new IllegalArgumentException("Unknown engine type");
        }
        log.info("Executing command: [{}]", execCommand);
        String logFile = String.join(File.separator, new String[]{seaTunnelHomePath, "log", jobInstanceId});
        String errorLogFile = String.join(File.separator, new String[]{seaTunnelHomePath, "log", jobInstanceId + "_error"});
        try (FileOutputStream logFileOutputStream = new FileOutputStream(logFile);
             FileOutputStream errorLogFileOutputStream = new FileOutputStream(errorLogFile)) {
            CommandLine commandLine = CommandLine.parse(execCommand);
            DefaultExecutor defaultExecutor = new DefaultExecutor();
            PumpStreamHandler pumpStreamHandler = new PumpStreamHandler(logFileOutputStream, errorLogFileOutputStream);
            defaultExecutor.setStreamHandler(pumpStreamHandler);
            int execute = defaultExecutor.execute(commandLine);
            if (execute == 0) {
                // job completed

            } else {
                // job failed
            }
        } catch (IOException e) {
            throw new RuntimeException("Execute SeaTunnel job failed", e);
        }
    }

    public JobInstance getJobInstance() {
        String host = executorCommandArgs.getHost();
        String targetURL = String.format("%s?jobDefineId=%s", host, executorCommandArgs.getJobId());
        HttpResponse httpResponse;
        try {
            httpResponse = httpClientProvider.doGet(targetURL);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Get job [%s] resource detail from SeaTunnel Web Server failed",
                            executorCommandArgs.getJobId()), e);
        }
        if (httpResponse == null) {
            throw new RuntimeException(
                    String.format("Get job [%s] resource detail from SeaTunnel Web Server is null",
                            executorCommandArgs.getJobId()));
        }
        String response = httpResponse.getContent();
        return JsonUtils.parseObject(response, JobInstance.class);
    }

    public String getSeaTunnelHomePath() {
        String currentJarPath = getCurrentJarPath();
        File file = new File(currentJarPath);
        return file.getParent();
    }

    private String getCurrentJarPath() {
        String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        if (System.getProperty("os.name").contains("dows")) {
            path = path.substring(1);
        }
        if (path.contains("jar")) {
            path = path.substring(0, path.lastIndexOf("."));
            return path.substring(0, path.lastIndexOf("/"));
        }
        return path.replace("target/classes/", "");
    }
}
