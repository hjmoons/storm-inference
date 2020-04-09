package dke.model;

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
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

import java.util.Map;

public class InferenceBolt extends BaseRichBolt {
    private Log LOG = LogFactory.getLog(InferenceBolt.class);

    private OutputCollector outputCollector;
    private SavedModelBundle savedModelBundle;
    private Session sess;
    public InferenceBolt() {

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.savedModelBundle = SavedModelBundle.load("/home/team1/hyojong/mnist", "serve");
        this.sess = savedModelBundle.session();
    }

    @Override
    public void execute(Tuple tuple) {
        String input = tuple.getString(0);
        float[][][][] data = getInputData(input);
        //float[][] result = loadedModel.run(input_data);
        Tensor x = Tensor.create(data);

        // Running session and get output tensor
        Tensor result = sess.runner()
                .feed("input:0", x)
                .fetch("output/Softmax:0")
                .run()
                .get(0);

        float[][] prob = (float[][]) result.copyTo(new float[1][10]);
        String result_value = setOutputData(prob);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("result", result_value);
        jsonObject.put("time", System.currentTimeMillis());

        outputCollector.emit(new Values(jsonObject));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }

    public float[][][][] getInputData(String input) {
        float[][][][] data = new float[1][28][28][1];
        String str = input.replaceAll("[\\[\\]]", "");
        String[] str_split = str.split(",");

        int count = 0;
        for(int i = 0; i < 28; i++) {
            for(int j = 0; j < 28; j++) {
                data[0][i][j][0] = Float.parseFloat(str_split[count]);
                count++;
            }
        }

        return data;
    }

    public String setOutputData(float[][] prob) {
        String value = "[";
        for(int i = 0; i < 10; i++) {
            value = value + Float.toString(prob[0][i]);
            if(i < 9)   value = value + ", ";
        }
        value = value + "]";

        return value;
    }
}
