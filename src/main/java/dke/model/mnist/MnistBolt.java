package dke.model.mnist;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

import java.util.Map;

public class MnistBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private SavedModelBundle savedModelBundle;
    private Session sess;
    private DataPreprocessing dataPreprocessing;

    private JSONParser jsonParser;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.dataPreprocessing = new DataPreprocessing();
        this.savedModelBundle = SavedModelBundle.load("/home/team1/hyojong/mnist", "serve");
        this.sess = savedModelBundle.session();
        this.jsonParser = new JSONParser();
    }

    @Override
    public void execute(Tuple tuple) {
        String input = tuple.getString(0);

        JSONObject inputJSON = null;
        String instances = null;
        Long inputTime = 0L;
        int number = 0;

        try {
            inputJSON = new JSONObject(input);
            instances = inputJSON.getString("instances");
            inputTime = inputJSON.getLong("inputTime");
            number = inputJSON.getInt("number");
        } catch (JSONException e) {
            e.printStackTrace();
        }

        float[][][][] data = dataPreprocessing.getInputData(instances);

        Tensor x = Tensor.create(data);

        // Running session and get output tensor
        Tensor result = sess.runner()
                .feed("input:0", x)
                .fetch("output/Softmax:0")
                .run()
                .get(0);

        float[][] prob = (float[][]) result.copyTo(new float[1][10]);
        //String result_value = dataPreprocessing.setOutputData(prob);

        OutputMnist outputMnist = new OutputMnist();
        outputMnist.setPredictions(prob);
        outputMnist.setInputTime(inputTime);
        outputMnist.setOutputTime(System.currentTimeMillis());
        outputMnist.setNumber(number);

        String outputData = null;

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            outputData = objectMapper.writeValueAsString(outputMnist);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        outputCollector.emit(new Values(outputData));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }
}
