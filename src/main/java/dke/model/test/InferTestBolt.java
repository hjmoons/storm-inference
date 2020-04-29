package dke.model.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dke.model.test.data.InputObj;
import dke.model.test.data.OutputObj;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

import java.io.IOException;
import java.util.Map;

public class InferTestBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private SavedModelBundle savedModelBundle;
    private Session sess;

    private String modelPath;
    private String modelInput;
    private String modelOutput;

    private ObjectMapper objectMapper;
    private InputObj inputObj;
    private OutputObj outputObj;

    public InferTestBolt(String modelPath, String modelInput, String modelOutput) {
        this.modelPath = modelPath;
        this.modelInput = modelInput;
        this.modelOutput = modelOutput;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.savedModelBundle = SavedModelBundle.load(modelPath, "serve");
        this.sess = savedModelBundle.session();

        this.objectMapper = new ObjectMapper();
        this.outputObj = new OutputObj();
    }

    @Override
    public void execute(Tuple tuple) {
        String inputJson = tuple.getString(0);
        String outputJson = null;

        try {
            inputObj = objectMapper.readValue(inputJson, InputObj.class);


            float[][][][] data = inputObj.getInstances();

            Tensor x = Tensor.create(data);

            // Running session and get output tensor
            Tensor result = sess.runner()
                    .feed(modelInput, x)
                    .fetch(modelOutput)
                    .run()
                    .get(0);

            float[][] prob = (float[][]) result.copyTo(new float[1][10]);

            outputObj.setPredictions(prob);
            outputObj.setInputTime(inputObj.getInputTime());
            outputObj.setOutputTime(System.currentTimeMillis());
            outputObj.setNumber(outputObj.getNumber());

            ObjectMapper objectMapper = new ObjectMapper();
            outputJson = objectMapper.writeValueAsString(outputObj);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
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
