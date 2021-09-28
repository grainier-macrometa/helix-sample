package co.macrometa.helix.sample.state;

import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class C8CEPStateModelFactory extends StateModelFactory<StateModel> {

    int _delay;
    String _instanceName;

    public C8CEPStateModelFactory(int delay) {
        this._instanceName = "";
        this._delay = delay;
    }

    public C8CEPStateModelFactory(String instanceName) {
        this._instanceName = "";
        this._instanceName = instanceName;
        this._delay = 10;
    }

    public C8CEPStateModelFactory(String instanceName, int delay) {
        this._instanceName = "";
        this._instanceName = instanceName;
        this._delay = delay;
    }

    public C8CEPStateModelFactory() {
        this(10);
    }

    public StateModel createNewStateModel(String resourceName, String partitionName) {
        C8CEPStateModel stateModel = new C8CEPStateModel();
        stateModel.setInstanceName(this._instanceName);
        stateModel.setDelay(this._delay);
        stateModel.setPartitionName(partitionName);
        return stateModel;
    }


}
