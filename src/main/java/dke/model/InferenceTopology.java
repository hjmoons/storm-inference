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

public class InferenceTopology {
    private Log LOG = LogFactory.getLog(InferenceTopology.class);

    private final int NUM_WORKERS = 8;
    private final int KAFKA_SPOUT_PARAL = 1;
    private final int PREPARING_BOLT_PARAL = 2;
    private final int INFERENCE_BOLT_PARAL = 4;
    private final int KAFKA_BOLT_PARAL = 1;

    public static void main(String[] args) {
        String topologyName = args[0];
        String inputTopic = args[1];
        String outputTopic = args[2];
        String zkHosts = "MN:2182,SN01:2182,SN02:2182,SN03:2182,SN04:2182,SN05:2182,SN06:2182,SN07:2182,SN08:2182";
        String bootstrap = "MN:49092,SN01:49092,SN02:49092,SN03:49092,SN04:49092,SN05:49092,SN06:49092,SN07:49092,SN08:49092";

        InferenceTopology inferenceTopology = new InferenceTopology();
        inferenceTopology.topology(topologyName, inputTopic, outputTopic, zkHosts, bootstrap);
    }

    public void topology(String topologyName, String inputTopic, String outputTopic, String zkhosts, String bootstrap) {
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaSpoutConfig(zkhosts, inputTopic));
        PreparingBolt preparingBolt = new PreparingBolt();
        InferenceBolt inferenceBolt = new InferenceBolt();
        KafkaBolt kafkabolt = new KafkaBolt().withProducerProperties(kafkaBoltConfig(bootstrap))
                .withTopicSelector(new DefaultTopicSelector(outputTopic))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-spout", kafkaSpout, KAFKA_SPOUT_PARAL);
        builder.setBolt("preparing-bolt", preparingBolt, PREPARING_BOLT_PARAL).shuffleGrouping("kafka-spout");
        builder.setBolt("inference-bolt", inferenceBolt, INFERENCE_BOLT_PARAL).shuffleGrouping("preparing-bolt");
        builder.setBolt("kafka-bolt", kafkabolt, KAFKA_BOLT_PARAL).shuffleGrouping("inference-bolt");            // Store Data to Kafka

        Config config = new Config();
        config.setNumWorkers(NUM_WORKERS);

        try {
            StormSubmitter.submitTopology(topologyName, config, builder.createTopology());

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
