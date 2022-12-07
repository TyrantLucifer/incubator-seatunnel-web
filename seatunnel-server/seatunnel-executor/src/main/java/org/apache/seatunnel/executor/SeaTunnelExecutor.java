package org.apache.seatunnel.executor;

import org.apache.seatunnel.executor.command.ExecutorCommandArgs;
import org.apache.seatunnel.executor.util.CommandLineUtils;

public class SeaTunnelExecutor {
    public static void main(String[] args) {
        ExecutorCommandArgs executorCommandArgs = CommandLineUtils.parseSeaTunnelExecutorArgs(args);
        System.out.println(executorCommandArgs);
    }
}
