package co.macrometa.helix.sample.state;

import co.macrometa.helix.sample.C8CEPClusteringSample;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;

@StateModelInfo(
        initialState = C8CEPClusteringSample.OFFLINE,
        states = {
                C8CEPClusteringSample.ACTIVE,
                C8CEPClusteringSample.STANDBY,
                C8CEPClusteringSample.OFFLINE,
                C8CEPClusteringSample.DROPPED
        }
)
public class C8CEPStateModel extends StateModel {

    int _transDelay = 0;
    String partitionName;
    String _instanceName = "";

    public C8CEPStateModel() {
    }

    public String getPartitionName() {
        return this.partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public void setDelay(int delay) {
        this._transDelay = Math.max(delay, 0);
    }

    public void setInstanceName(String instanceName) {
        this._instanceName = instanceName;
    }

    private void sleep() {
        try {
            Thread.sleep(this._transDelay);
        } catch (Exception var2) {
            var2.printStackTrace();
        }

    }

    @Transition(from = C8CEPClusteringSample.OFFLINE, to = C8CEPClusteringSample.STANDBY)
    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
        String partitionName = message.getPartitionName();
        String instanceName = message.getTgtName();
        System.out.printf("%s transitioning from %s to %s for %s%n", instanceName, message.getFromState(), message.getToState(), partitionName);
        this.sleep();
    }

    @Transition(from = C8CEPClusteringSample.STANDBY, to = C8CEPClusteringSample.OFFLINE)
    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
        String partitionName = message.getPartitionName();
        String instanceName = message.getTgtName();
        System.out.printf("%s transitioning from %s to %s for %s%n", instanceName, message.getFromState(), message.getToState(), partitionName);
        this.sleep();
    }

    @Transition(from = C8CEPClusteringSample.STANDBY, to = C8CEPClusteringSample.ACTIVE)
    public void onBecomeOnlineFromStandby(Message message, NotificationContext context) {
        String partitionName = message.getPartitionName();
        String instanceName = message.getTgtName();
        System.out.printf("%s transitioning from %s to %s for %s%n", instanceName, message.getFromState(), message.getToState(), partitionName);
        this.sleep();
    }

    @Transition(from = C8CEPClusteringSample.ACTIVE, to = C8CEPClusteringSample.STANDBY)
    public void onBecomeStandbyFromOnline(Message message, NotificationContext context) {
        String partitionName = message.getPartitionName();
        String instanceName = message.getTgtName();
        System.out.printf("%s transitioning from %s to %s for %s%n", instanceName, message.getFromState(), message.getToState(), partitionName);
        this.sleep();
    }

    @Transition(from = C8CEPClusteringSample.OFFLINE, to = C8CEPClusteringSample.DROPPED)
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
        String partitionName = message.getPartitionName();
        String instanceName = message.getTgtName();
        System.out.println(instanceName + " becomes DROPPED from OFFLINE for " + partitionName);
        this.sleep();
    }
}
