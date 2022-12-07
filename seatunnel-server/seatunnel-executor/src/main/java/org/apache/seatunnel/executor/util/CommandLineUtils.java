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

package org.apache.seatunnel.executor.util;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.UnixStyleUsageFormatter;
import org.apache.seatunnel.core.starter.seatunnel.constant.SeaTunnelConstant;
import org.apache.seatunnel.executor.command.ExecutorCommandArgs;

public class CommandLineUtils {

    public static final String EXECUTOR_SHELL_NAME = "seatunnel-executor.sh";
    public static final String SEATUNNEL_STARTER_SHELL_NAME = "seatunnel.sh";
    public static final String SPARK_STARTER_SHELL_NAME = "start-seatunnel-spark-connector-v2.sh";
    public static final String FLINK_STARTER_SHELL_NAME = "start-seatunnel-flink-connector-v2.sh";

    private CommandLineUtils() {
        throw new UnsupportedOperationException("CommandLineUtils is a utility class and cannot be instantiated");
    }

    public static ExecutorCommandArgs parseSeaTunnelExecutorArgs(String[] args) {
        ExecutorCommandArgs executorCommandArgs = new ExecutorCommandArgs();
        JCommander jCommander = getJCommander(args, executorCommandArgs);
        if (executorCommandArgs.isHelp()) {
            printHelp(jCommander);
        }
        return executorCommandArgs;
    }

    private static void printHelp(JCommander jCommander) {
        jCommander.setUsageFormatter(new UnixStyleUsageFormatter(jCommander));
        jCommander.usage();
        System.exit(SeaTunnelConstant.USAGE_EXIT_CODE);
    }

    private static JCommander getJCommander(String[] args, Object serverCommandArgs) {
        return JCommander.newBuilder()
            .programName(EXECUTOR_SHELL_NAME)
            .addObject(serverCommandArgs)
            .acceptUnknownOptions(true)
            .args(args)
            .build();
    }
}
