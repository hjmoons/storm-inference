package dke.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dke.model.data.InstObj;
import dke.model.data.PredObj;
import org.apache.commons.io.IOUtils;
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
import java.io.IOException;
import java.util.Map;

public class InferenceBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private SavedModelBundle savedModelBundle;
    private Session sess;

    private ObjectMapper objectMapper;
    private InstObj instObj;
    private PredObj predObj;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        ClassPathResource resource = new ClassPathResource("model/saved_model.pb");

        try {
            File modelFile = new File("./saved_model.pb");
            IOUtils.copy(resource.getInputStream(), new FileOutputStream(modelFile));
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.savedModelBundle = SavedModelBundle.load("./", "serve");
        sess = savedModelBundle.session();

        this.objectMapper = new ObjectMapper();
        this.predObj = new PredObj();
    }

    @Override
    public void execute(Tuple tuple) {
        String inputJson = tuple.getString(0);

        try {
            instObj = objectMapper.readValue(inputJson, InstObj.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        float[][][][] data = instObj.getInstances();

        Tensor x = Tensor.create(data);

        // Running session and get output tensor
        Tensor result = sess.runner()
                .feed("input:0", x)
                .fetch("output/Softmax:0")
                .run()
                .get(0);

        float[][] prob = (float[][]) result.copyTo(new float[1][10]);

        predObj.setPredictions(prob);

        String outputJson = null;

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            outputJson = objectMapper.writeValueAsString(predObj);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        outputCollector.emit(new Values(outputJson));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }
}
