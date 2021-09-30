package co.macrometa.helix.sample;

import co.macrometa.helix.sample.process.C8CEPInstance;
import org.apache.helix.HelixAdmin;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

import static co.macrometa.helix.sample.util.Constants.CLUSTER_NAME;
import static co.macrometa.helix.sample.util.Constants.STATE_MODEL_NAME;
import static co.macrometa.helix.sample.util.Constants.ZK_ADDRESS;

public class C8CEPClusteringSample {

    // docker run --name helix-zookeeper --restart always -d -p 2181:2181 -p 8080:8080 zookeeper
    // or remote zookeeper URL
    private static int NUM_NODES = 3;
    private static final String RESOURCE_NAME = "StreamApp1-" + UUID.randomUUID();
    private static final String RESOURCE_NAME_2 = "StreamApp2";
    private static final int NUM_PARTITIONS = 1;
    private static final int NUM_REPLICAS = 3;

    // states
    public static final String ONLINE = "ONLINE";
    public static final String STANDBY = "STANDBY";
    public static final String OFFLINE = "OFFLINE";
    public static final String DROPPED = "DROPPED";

    private static List<InstanceConfig> C8CEP_INSTANCES;
    private static List<C8CEPInstance> STREAM_WORKERS;
    private static HelixAdmin admin;

    static {
        C8CEP_INSTANCES = new ArrayList<InstanceConfig>();
        STREAM_WORKERS = new ArrayList<C8CEPInstance>();
        for (int i = 0; i < NUM_NODES; i++) {
            int port = 12000 + i;
            InstanceConfig instanceConfig = new InstanceConfig("c8cep_on_localhost_" + port);
            instanceConfig.setHostName("localhost");
            instanceConfig.setPort("" + port);
            instanceConfig.setInstanceEnabled(true);
            C8CEP_INSTANCES.add(instanceConfig);
        }
    }

    public static void main(String[] args) throws Exception {
        setupCluster();
        startNodes();
        startController();
        Thread.sleep(5000);
        startController();
        Thread.sleep(5000);
        startController();
        Thread.sleep(5000);
        printState("After starting 2 nodes", RESOURCE_NAME);
        printState("After starting 2 nodes", RESOURCE_NAME_2);
        addNode();
        Thread.sleep(5000);
        printState("After adding a third node", RESOURCE_NAME);
        printState("After adding a third node", RESOURCE_NAME_2);
        stopNode(0);
//        Thread.sleep(10);
//        reConnectNode(0);
        Thread.sleep(5000);
        printState("After the 1st node stops/crashes", RESOURCE_NAME);
        printState("After the 1st node stops/crashes", RESOURCE_NAME_2);
        stopNode(1);

        Thread.sleep(5000);
        printState("After the 2nd node stops/crashes", RESOURCE_NAME);
        printState("After the 2nd node stops/crashes", RESOURCE_NAME_2);
        stopNode(2);
        Thread.sleep(5000);
        printState("After the 2nd node stops/crashes", RESOURCE_NAME);
        printState("After the 2nd node stops/crashes", RESOURCE_NAME_2);

        Thread.currentThread().join();
        System.exit(0);
    }

    public static void setupCluster() {
        // Create admin
        admin = new ZKHelixAdmin(ZK_ADDRESS);

        // Create cluster
        echo("Creating cluster: " + CLUSTER_NAME);
        admin.addCluster(CLUSTER_NAME, true);

        // Allow participant auto join.
        HelixConfigScope scope =
                new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(CLUSTER_NAME)
                        .build();
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");
        admin.setConfig(scope, properties);

        // Add nodes to the cluster
        echo("Adding " + NUM_NODES + " participants to the cluster");
        for (int i = 0; i < NUM_NODES; i++) {
            admin.addInstance(CLUSTER_NAME, C8CEP_INSTANCES.get(i));
            echo("\t Added participant: " + C8CEP_INSTANCES.get(i).getInstanceName());
        }

        // Add a state model
        StateModelDefinition myStateModel = defineStateModel();
        echo("Configuring StateModel: " + STATE_MODEL_NAME);
        admin.addStateModelDef(CLUSTER_NAME, STATE_MODEL_NAME, myStateModel);

        // Add a resource 2
        addResource(RESOURCE_NAME);

        admin.getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME);

        // Add a resource 2
        addResource(RESOURCE_NAME_2);

    }

    private static void addResource(String resourceName) {
        echo(String.format("Adding a resource %s with %s partitions and %s replicas", resourceName, NUM_PARTITIONS, NUM_REPLICAS));

        if (admin.getResourceIdealState(CLUSTER_NAME, resourceName) == null) {
            FullAutoModeISBuilder idealStateBuilder = new FullAutoModeISBuilder(resourceName);
            idealStateBuilder.setStateModel(STATE_MODEL_NAME).setNumPartitions(NUM_PARTITIONS).setNumReplica(NUM_REPLICAS).setMaxPartitionsPerNode(2);
            idealStateBuilder.setRebalanceStrategy("org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy");
            idealStateBuilder.setRebalancerClass("org.apache.helix.controller.rebalancer.DelayedAutoRebalancer");

            // TODO: check this
            idealStateBuilder.enableDelayRebalance();
            idealStateBuilder.setRebalanceDelay(1000);

//        JobConfig.Builder myJobCfgBuilder = new JobConfig.Builder();
//        JobConfig myJobCfg = myJobCfgBuilder.build();
//
//        myJobCfgBuilder.setTargetResource(resourceName);

            admin.addResource(CLUSTER_NAME, resourceName, idealStateBuilder.build());
            // this will set up the ideal state, it calculates the preference list for each partition similar to consistent hashing
            admin.rebalance(CLUSTER_NAME, resourceName, NUM_REPLICAS);
        }

    }

    private static StateModelDefinition defineStateModel() {
        StateModelDefinition.Builder builder = new StateModelDefinition.Builder(STATE_MODEL_NAME);
        // Add states and their rank to indicate priority. Lower the rank higher the priority
        builder.addState(ONLINE, 1);
        builder.addState(STANDBY, 2);
        builder.addState(OFFLINE);
        builder.addState(DROPPED);

        // Initial State
        builder.initialState(OFFLINE);

        // Add transitions between the states.
        builder.addTransition(OFFLINE, STANDBY);
        builder.addTransition(STANDBY, OFFLINE);
        builder.addTransition(STANDBY, ONLINE);
        builder.addTransition(ONLINE, STANDBY);
        builder.addTransition(OFFLINE, DROPPED);

        // Set Static constraint for Online state (only 1 ONLINE state).
        builder.upperBound(ONLINE, 1);
        // Set dynamic constraint for Standby state, R means it should be derived based on the replication factor.
        builder.dynamicUpperBound(STANDBY, "R");

        StateModelDefinition statemodelDefinition = builder.build();
        assert statemodelDefinition.isValid();
        return statemodelDefinition;
    }

    public static void startNodes() throws Exception {
        echo("Starting Participants");
        for (int i = 0; i < NUM_NODES; i++) {
            C8CEPInstance process = new C8CEPInstance(C8CEP_INSTANCES.get(i).getId());
            STREAM_WORKERS.add(process);
            process.start();
            echo("\t Started Participant: " + C8CEP_INSTANCES.get(i).getId());
        }
    }

    public static void startController() {
        // start controller
        echo("Starting Helix Controller");
        // In distributed setup, controllerName should be unique
        String controllerName = "CEPControllerName-" + UUID.randomUUID();
        HelixControllerMain.startHelixController(ZK_ADDRESS, CLUSTER_NAME, controllerName, HelixControllerMain.DISTRIBUTED);
    }

    private static void addNode() throws Exception {
        NUM_NODES = NUM_NODES + 1;
        int port = 12000 + NUM_NODES - 1;
        InstanceConfig instanceConfig = new InstanceConfig("localhost_" + port);
        instanceConfig.setHostName("localhost");
        instanceConfig.setPort("" + port);
        instanceConfig.setInstanceEnabled(true);
        echo("ADDING NEW NODE :" + instanceConfig.getInstanceName()
                + ". Partitions will move from old nodes to the new node.");
        admin.addInstance(CLUSTER_NAME, instanceConfig);
        C8CEP_INSTANCES.add(instanceConfig);
        C8CEPInstance process = new C8CEPInstance(instanceConfig.getInstanceName());
        STREAM_WORKERS.add(process);
        process.start();
    }

    private static void stopNode(int nodeId) {
        echo("STOPPING " + C8CEP_INSTANCES.get(nodeId).getInstanceName()
                + ". Mastership will be transferred to the remaining nodes");
        STREAM_WORKERS.get(nodeId).stop();
    }

    private static void reConnectNode(int nodeId) throws Exception {
        echo("RECONNECT " + C8CEP_INSTANCES.get(nodeId).getInstanceName()
                + ". Mastership will be transferred to the remaining nodes");
        STREAM_WORKERS.get(nodeId).start();
    }

    public static void echo(Object obj) {
        System.out.println(obj);
    }

    private static void printState(String msg, String resourceName) {
        System.out.println("CLUSTER STATE: " + msg);
        ExternalView resourceExternalView = admin.getResourceExternalView(CLUSTER_NAME, resourceName);
        TreeSet<String> sortedSet = new TreeSet<String>(resourceExternalView.getPartitionSet());
        StringBuilder sb = new StringBuilder("\t\t\t");
        for (int i = 0; i < NUM_NODES; i++) {
            sb.append(C8CEP_INSTANCES.get(i).getInstanceName()).append("\t\t");
        }
        System.out.println(sb);
        for (String partitionName : sortedSet) {
            sb.delete(0, sb.length() - 1);
            sb.append(partitionName).append("\t");
            for (int i = 0; i < NUM_NODES; i++) {
                Map<String, String> stateMap = resourceExternalView.getStateMap(partitionName);
                if (stateMap != null && stateMap.containsKey(C8CEP_INSTANCES.get(i).getInstanceName())) {
                    sb.append(stateMap.get(C8CEP_INSTANCES.get(i).getInstanceName())).append(
                            "\t\t\t\t\t\t\t");
                } else {
                    sb.append("-").append("\t\t\t\t\t\t\t\t");
                }
            }
            System.out.println(sb);
        }
        System.out.println("###################################################################");
    }
}
