package dke.model;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class MainTopology {
    private Log LOG = LogFactory.getLog(MainTopology.class);

    private final int NUM_WORKERS = 8;
    private final int KAFKA_SPOUT_PARAL = 2;
    private final int INFERENCE_BOLT_PARAL = 4;
    private final int KAFKA_BOLT_PARAL = 2;

    private static final long serialVersionUID = 1L;

    public static void main(String[] args) {
        String zkHosts = "MN:42181,SN01:42181,SN02:42181,SN03:42181,SN04:42181,SN05:42181,SN06:42181,SN07:42181,SN08:42181";
        String bootstrap = "MN:49092,SN01:49092,SN02:49092,SN03:49092,SN04:49092,SN05:49092,SN06:49092,SN07:49092,SN08:49092";

        String topologyName = args[0];
        String inputTopic = args[1];
        String outputTopic = args[2];

        MainTopology mainTopology = new MainTopology();
        mainTopology.mainTopology(topologyName, inputTopic, outputTopic, zkHosts, bootstrap);
    }

    /**
     * Set the topology to be executed
     * @param topologyName name to represent topology
     * @param inputTopic topic to pull input data
     * @param outputTopic topic to push output data
     * @param zkhosts zookeeper host to use kafka
     * @param bootstrap broker list to use kafka
     */
    public void mainTopology(String topologyName, String inputTopic, String outputTopic, String zkhosts, String bootstrap) {
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaSpoutConfig(zkhosts, inputTopic));
        InferenceBolt inferenceBolt = new InferenceBolt();
        KafkaBolt kafkabolt = new KafkaBolt().withProducerProperties(kafkaBoltConfig(bootstrap))
                .withTopicSelector(new DefaultTopicSelector(outputTopic))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-spout", kafkaSpout, KAFKA_SPOUT_PARAL);
        builder.setBolt("inference-bolt", inferenceBolt, INFERENCE_BOLT_PARAL).shuffleGrouping("kafka-spout");
        builder.setBolt("kafka-bolt", kafkabolt, KAFKA_BOLT_PARAL).shuffleGrouping("cifar-bolt");            // Store Data to Kafka

        Config config = new Config();
        config.setNumWorkers(NUM_WORKERS);

        try {
            StormSubmitter.submitTopology(topologyName, config, builder.createTopology());

            Thread.sleep(60 * 60 * 1000);

            Map<String, Object> conf = Utils.readStormConfig();
            Nimbus.Client client = NimbusClient.getConfiguredClient(conf).getClient();
            KillOptions killOpts = new KillOptions();
            killOpts.set_wait_secs(0);
            client.killTopologyWithOpts(topologyName, killOpts);

        } catch (AlreadyAliveException e) {
            LOG.info(e.get_msg());
        } catch (InvalidTopologyException e) {
            LOG.info(e.get_msg());
        } catch (AuthorizationException e) {
            LOG.info(e.get_msg());
        } catch (NotAliveException e) {
            LOG.info(e.get_msg());
        } catch (TException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /* Kafka Spout Configuration */
    public SpoutConfig kafkaSpoutConfig(String zkhosts, String inputTopic) {
        BrokerHosts brokerHosts = new ZkHosts(zkhosts);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig(brokerHosts, inputTopic, "/" + inputTopic,
                UUID.randomUUID().toString());
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();    // To pull the latest data in topic
        kafkaSpoutConfig.ignoreZkOffsets = true;
        kafkaSpoutConfig.maxOffsetBehind = 0;

        return kafkaSpoutConfig;
    }

    /* KafkaBolt Configuration */
    public Properties kafkaBoltConfig(String bootstrap) {
        Properties kafkaBoltConfig = new Properties();
        kafkaBoltConfig.put("metadata.broker.list", bootstrap);
        kafkaBoltConfig.put("bootstrap.servers", bootstrap);
        kafkaBoltConfig.put("acks", "1");
        kafkaBoltConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaBoltConfig.put("value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer");

        return kafkaBoltConfig;
    }
}
