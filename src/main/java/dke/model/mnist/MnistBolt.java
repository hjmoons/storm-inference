package dke.model.mnist;

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

public class MnistBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private SavedModelBundle savedModelBundle;
    private Session sess;
    private DataPreprocessing dataPreprocessing;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.dataPreprocessing = new DataPreprocessing();
        this.savedModelBundle = SavedModelBundle.load("/home/team1/hyojong/mnist", "serve");
        this.sess = savedModelBundle.session();
    }

    @Override
    public void execute(Tuple tuple) {
        String input = tuple.getString(0);
        float[][][][] data = dataPreprocessing.getInputData(input);

        Tensor x = Tensor.create(data);

        // Running session and get output tensor
        Tensor result = sess.runner()
                .feed("input:0", x)
                .fetch("output/Softmax:0")
                .run()
                .get(0);

        float[][] prob = (float[][]) result.copyTo(new float[1][10]);
        String result_value = dataPreprocessing.setOutputData(prob);

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
}
