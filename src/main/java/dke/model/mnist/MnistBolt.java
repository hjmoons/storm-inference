package dke.model.mnist;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
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
        try {
            inputJSON = (JSONObject) jsonParser.parse(input);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String input_time = (String) inputJSON.get("time");
        String input_data = (String) inputJSON.get("data");
        String input_number = (String) inputJSON.get("number");

        float[][][][] data = dataPreprocessing.getInputData(input_data);

        Tensor x = Tensor.create(data);

        // Running session and get output tensor
        Tensor result = sess.runner()
                .feed("input:0", x)
                .fetch("output/Softmax:0")
                .run()
                .get(0);

        float[][] prob = (float[][]) result.copyTo(new float[1][10]);
        String result_value = dataPreprocessing.setOutputData(prob);

        JSONObject outputJSon = new JSONObject();
        outputJSon.put("number", input_number);
        outputJSon.put("result", result_value);
        outputJSon.put("inputtime", input_time);
        outputJSon.put("outputtime", String.valueOf(System.currentTimeMillis()));

        outputCollector.emit(new Values(outputJSon));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }
}
