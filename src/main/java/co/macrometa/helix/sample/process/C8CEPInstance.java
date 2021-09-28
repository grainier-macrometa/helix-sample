package co.macrometa.helix.sample.process;

import co.macrometa.helix.sample.state.C8CEPStateModelFactory;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.participant.StateMachineEngine;

import static co.macrometa.helix.sample.util.Constants.CLUSTER_NAME;
import static co.macrometa.helix.sample.util.Constants.STATE_MODEL_NAME;
import static co.macrometa.helix.sample.util.Constants.ZK_ADDRESS;

// This is participant
public class C8CEPInstance {

    private final String instanceName;
    private HelixManager manager;

    public C8CEPInstance(String instanceName) {
        this.instanceName = instanceName;
    }

    public void start() throws Exception {
        manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, instanceName, InstanceType.PARTICIPANT, ZK_ADDRESS);
        C8CEPStateModelFactory stateModelFactory = new C8CEPStateModelFactory(instanceName);
        StateMachineEngine stateMach = manager.getStateMachineEngine();
        stateMach.registerStateModelFactory(STATE_MODEL_NAME, stateModelFactory);
        manager.connect();
    }

    public void stop() {
        manager.disconnect();
    }
}
