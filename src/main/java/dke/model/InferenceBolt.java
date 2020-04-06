package dke.model;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.springframework.core.io.ClassPathResource;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;

public class InferenceBolt extends BaseRichBolt {
    private Log LOG = LogFactory.getLog(InferenceBolt.class);

    private OutputCollector outputCollector;
    private SavedModelBundle savedModelBundle;
    private Session sess;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        ClassPathResource resource = new ClassPathResource("models/cnn/saved_model.pb");

        try {
            File modelFile = new File("./saved_model.pb");
            IOUtils.copy(resource.getInputStream(), new FileOutputStream(modelFile));
        } catch (Exception e) {
            e.printStackTrace();
        }
        savedModelBundle = SavedModelBundle.load("./", "serve");
        sess = savedModelBundle.session();
    }

    @Override
    public void execute(Tuple tuple) {
        String mnist_img = (String) tuple.getValueByField("img");

        //create an input Tensor
        Tensor x = Tensor.create(mnist_img);

        // Running session and get output tensor
        Tensor result = sess.runner()
                .feed("input_1:0", x)
                .fetch("output_1/Sigmoid:0")
                .run()
                .get(0);

        float[][] prob = (float[][]) result.copyTo(new float[1][1]);
        LOG.info("Result value: " + prob[0][0]);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("text", tuple.getValueByField("text"));
        jsonObject.put("result", prob[0][0]);
        jsonObject.put("time", System.currentTimeMillis());

        outputCollector.emit(new Values(jsonObject));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }
}
