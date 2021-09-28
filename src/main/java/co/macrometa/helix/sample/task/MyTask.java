package co.macrometa.helix.sample.task;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.UserContentStore;

public class MyTask extends UserContentStore implements Task {

    @Override
    public TaskResult run() {
        putUserContent("KEY", "WORKFLOWVALUE", Scope.WORKFLOW);
        putUserContent("KEY", "JOBVALUE", Scope.JOB);
        putUserContent("KEY", "TASKVALUE", Scope.TASK);
        String taskValue = getUserContent("KEY", Scope.TASK);
//        return new TaskResult(TaskResult.Status.FAILED, "ERROR MESSAGE OR OTHER INFORMATION");
//        return new TaskResult(TaskResult.Status.FATAL_FAILED, "DO NOT WANT TO RETRY, ERROR MESSAGE");
        return new TaskResult(TaskResult.Status.COMPLETED, "TASK COMPLETED");
    }

    @Override
    public void cancel() {

    }

}
