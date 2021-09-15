package co.macrometa.helix.sample;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class MasterSlaveStateModelFactory extends StateModelFactory<StateModel> {

    int _delay;
    String _instanceName;

    public MasterSlaveStateModelFactory(int delay) {
        this._instanceName = "";
        this._delay = delay;
    }

    public MasterSlaveStateModelFactory(String instanceName) {
        this._instanceName = "";
        this._instanceName = instanceName;
        this._delay = 10;
    }

    public MasterSlaveStateModelFactory(String instanceName, int delay) {
        this._instanceName = "";
        this._instanceName = instanceName;
        this._delay = delay;
    }

    public MasterSlaveStateModelFactory() {
        this(10);
    }

    public StateModel createNewStateModel(String resourceName, String partitionName) {
        MasterSlaveStateModelFactory.MasterSlaveStateModel stateModel = new MasterSlaveStateModelFactory.MasterSlaveStateModel();
        stateModel.setInstanceName(this._instanceName);
        stateModel.setDelay(this._delay);
        stateModel.setPartitionName(partitionName);
        return stateModel;
    }

    public static class MasterSlaveStateModel extends StateModel {

        int _transDelay = 0;
        String partitionName;
        String _instanceName = "";

        public MasterSlaveStateModel() {
        }

        public String getPartitionName() {
            return this.partitionName;
        }

        public void setPartitionName(String partitionName) {
            this.partitionName = partitionName;
        }

        public void setDelay(int delay) {
            this._transDelay = delay > 0 ? delay : 0;
        }

        public void setInstanceName(String instanceName) {
            this._instanceName = instanceName;
        }

        public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
            System.out.println(this._instanceName + " transitioning from " + message.getFromState() + " to " + message.getToState() + " for " + this.partitionName);
            this.sleep();
        }

        private void sleep() {
            try {
                Thread.sleep((long) this._transDelay);
            } catch (Exception var2) {
                var2.printStackTrace();
            }

        }

        public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
            System.out.println(this._instanceName + " transitioning from " + message.getFromState() + " to " + message.getToState() + " for " + this.partitionName);
            this.sleep();
        }

        public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
            System.out.println(this._instanceName + " transitioning from " + message.getFromState() + " to " + message.getToState() + " for " + this.partitionName);
            this.sleep();
        }

        public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
            System.out.println(this._instanceName + " transitioning from " + message.getFromState() + " to " + message.getToState() + " for " + this.partitionName);
            this.sleep();
        }

        public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
            System.out.println(this._instanceName + " Dropping partition " + this.partitionName);
            this.sleep();
        }
    }
}
