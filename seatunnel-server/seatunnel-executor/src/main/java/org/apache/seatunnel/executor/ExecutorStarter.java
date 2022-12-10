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

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.executor.command.ExecutorCommandArgs;
import org.apache.seatunnel.executor.job.JobInstance;
import org.apache.seatunnel.executor.util.CommandLineUtils;
import org.apache.seatunnel.executor.util.HttpClientProvider;
import org.apache.seatunnel.executor.util.HttpParameter;
import org.apache.seatunnel.executor.util.HttpResponse;
import org.apache.seatunnel.executor.util.MultiOutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class ExecutorStarter {
    private static final String TMP_JOB_CONFIG_DIR = "/tmp/job";
    private static final String SEATUNNEL_HOME_STRING = "SEATUNNEL_HOME";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Pattern JOB_ID_PATTERN = Pattern.compile("start submit job, job id: ([0-9]*),");
    private final HttpParameter httpParameter = new HttpParameter();
    private final HttpClientProvider httpClientProvider = new HttpClientProvider(httpParameter);
    private final ExecutorCommandArgs executorCommandArgs;

    public ExecutorStarter(ExecutorCommandArgs executorCommandArgs) {
        this.executorCommandArgs = executorCommandArgs;
    }

    public void start() throws Exception {
        // Get job instance from SeaTunnel Web Service
        JobInstance jobInstance = getJobInstance();
        String jobInstanceId = jobInstance.getJobInstanceId();
        // Write job temp config file
        String jobConfigFile = String.join(File.separator, new String[]{TMP_JOB_CONFIG_DIR, jobInstanceId}) + ".conf";
        FileUtils.createNewFile(jobConfigFile);
        try (FileOutputStream fileOutputStream = new FileOutputStream(jobConfigFile)) {
            fileOutputStream.write(jobInstance.getJobConfig().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            String errorMsg = String.format("Generate temp job config file [%s] failed", jobConfigFile);
            throw new RuntimeException(errorMsg, e);
        }
        // Based on different engine to join different command
        JobInstance.EngineType engineType = JobInstance.EngineType.valueOf(jobInstance.getEngine().toString().toUpperCase());
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
        executeJob(jobInstanceId, seaTunnelHomePath, execCommand);
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private void executeJob(String jobInstanceId, String seaTunnelHomePath, String execCommand) {
        log.info("Executing command: [{}]", execCommand);
        String logFile = String.join(File.separator, new String[]{seaTunnelHomePath, "logs", "executor", jobInstanceId}) + ".log";
        FileUtils.createNewFile(logFile);
        String errorLogFile = String.join(File.separator, new String[]{seaTunnelHomePath, "logs", "executor", jobInstanceId}) + ".err.log";
        FileUtils.createNewFile(logFile);
        PrintStream os = System.out;
        PrintStream error = System.err;
        String jobEngineId = "";
        try (
            FileOutputStream logFileOutputStream = new FileOutputStream(logFile);
            PipedOutputStream pipedOutputStream = new PipedOutputStream();
            MultiOutputStream multiOutputStream = new MultiOutputStream(logFileOutputStream, System.out, pipedOutputStream);
            FileOutputStream errorLogFileOutputStream = new FileOutputStream(errorLogFile);
            MultiOutputStream multiErrorStream = new MultiOutputStream(errorLogFileOutputStream, System.err)) {
            System.setOut(new PrintStream(multiOutputStream));
            System.setErr(new PrintStream(multiErrorStream));
            CommandLine commandLine = CommandLine.parse(execCommand);
            DefaultExecutor defaultExecutor = new DefaultExecutor();
            PumpStreamHandler pumpStreamHandler = new PumpStreamHandler(System.out, System.err);
            defaultExecutor.setStreamHandler(pumpStreamHandler);
            CompletableFuture<String> engineIdFuture = CompletableFuture.supplyAsync(() -> findJobEngineId(pipedOutputStream, jobInstanceId));
            defaultExecutor.execute(commandLine);
            jobEngineId = engineIdFuture.get();
            System.setOut(os);
            System.setErr(error);
        } catch (Exception e) {
            System.setOut(os);
            System.setErr(error);
            throw new RuntimeException("Execute SeaTunnel job failed", e);
        } finally {
            if (StringUtils.isNotEmpty(jobEngineId)) {
                sendFeedback(jobInstanceId, jobEngineId);
            }
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private static String findJobEngineId(PipedOutputStream pipedOutputStream, String jobInstanceId) {
        try (PipedInputStream pipedInputStream = new PipedInputStream(pipedOutputStream);
             InputStreamReader inputStreamReader = new InputStreamReader(pipedInputStream);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
            String line;
            while (true) {
                if ((line = bufferedReader.readLine()) != null) {
                    Matcher matcher = JOB_ID_PATTERN.matcher(line);
                    if (matcher.find()) {
                        String jobEngineId = matcher.group(1);
                        log.info("Find JobEngineId=[{}], JobInstanceId=[{}]", jobEngineId, jobInstanceId);
                        return jobEngineId;
                    }
                } else {
                    Thread.sleep(500);
                }
            }
        } catch (Exception e) {
            throw new SeaTunnelException(e);
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private void sendFeedback(String jobInstanceId, String jobEngineId) {
        String host = executorCommandArgs.getHost();
        String targetURL = String.format("%s/api/v1/job/executor/complete", host);
        Map<String, String> params = new HashMap<>();
        params.put("jobInstanceId", jobInstanceId);
        params.put("projectCode", executorCommandArgs.getProjectCode());
        params.put("jobEngineId", jobEngineId);
        HttpResponse httpResponse = getDataFromServer(targetURL, params);
        if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
            log.info("send job completed feedback success");
        } else {
            throw new SeaTunnelException(String.format("send job completed feedback failed, response code: %s, response body: %s",
                httpResponse.getCode(), httpResponse.getContent()));
        }
    }

    public JobInstance getJobInstance() {
        String host = executorCommandArgs.getHost();
        String targetURL = String.format("%s/api/v1/job/executor/resource", host);
        Map<String, String> params = new HashMap<>();
        params.put("jobDefineId", executorCommandArgs.getJobId());
        params.put("projectCode", executorCommandArgs.getProjectCode());
        HttpResponse httpResponse = getDataFromServer(targetURL, params);
        String response = httpResponse.getContent();
        Map<String, Object> data = getDataFromResponse(response);
        JobInstance jobInstance = new JobInstance();
        jobInstance.setJobInstanceId(data.get("jobInstanceId").toString());
        jobInstance.setJobConfig(data.get("jobConfig").toString());
        jobInstance.setEngine(JobInstance.EngineType.valueOf(data.get("engine").toString().toUpperCase()));
        return jobInstance;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private HttpResponse getDataFromServer(String targetURL, Map<String, String> params) {
        Map<String, String> header = new HashMap<>();
        if (StringUtils.isNotEmpty(executorCommandArgs.getToken())) {
            header.put("token", executorCommandArgs.getToken());
        }
        HttpResponse httpResponse;
        try {
            httpResponse = httpClientProvider.doGet(targetURL, header, params);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Query job [%s] from SeaTunnel Web Server failed, url [%s]",
                executorCommandArgs.getJobId(), targetURL), e);
        }
        if (httpResponse == null) {
            throw new RuntimeException(String.format("Query job [%s] from SeaTunnel Web Server is null, url [%s]",
                executorCommandArgs.getJobId(), targetURL));
        }
        if (httpResponse.getCode() < 200 || httpResponse.getCode() >= 300) {
            throw new SeaTunnelException(String.format("query job info failed, response code: %s, response body: %s",
                httpResponse.getCode(), httpResponse.getContent()));
        }
        return httpResponse;
    }

    private Map<String, Object> getDataFromResponse(String response) {
        try {
            Map<String, Object> responseValue = OBJECT_MAPPER.readValue(response, Map.class);
            return (Map<String, Object>) responseValue.get("data");
        } catch (Exception e) {
            throw new SeaTunnelException(e);
        }
    }

    public String getSeaTunnelHomePath() {
        String home = System.getenv(SEATUNNEL_HOME_STRING);
        if (StringUtils.isNotEmpty(home)) {
            return home;
        }
        home = System.getProperty(SEATUNNEL_HOME_STRING);
        if (StringUtils.isNotEmpty(home)) {
            return home;
        }
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
