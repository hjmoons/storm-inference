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

    /* model variable */
    private SavedModelBundle savedModelBundle;
    private Session sess;

    /* data variable */
    private ObjectMapper objectMapper;
    private InstObj instObj;
    private PredObj predObj;

    /**
     * 프로젝트의 resource 폴더에 저장된 모델(.pb)을 불러온 후, 모델의 세션을 실행시킨다.
     * @param map
     * @param topologyContext
     * @param outputCollector
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        /* 프로젝트 리소스 폴더에 존재하는 딥러닝 모델을 볼트가 실행되는 노드에 저장한다. */
        try {
            ClassPathResource resource = new ClassPathResource("model/saved_model.pb");
            File modelFile = new File("./saved_model.pb");
            IOUtils.copy(resource.getInputStream(), new FileOutputStream(modelFile));
        } catch (Exception e) {
            e.printStackTrace();
        }

        /* model load */
        this.savedModelBundle = SavedModelBundle.load("./", "serve");
        sess = savedModelBundle.session();

        this.objectMapper = new ObjectMapper();
        this.predObj = new PredObj();
    }

    /**
     * 카프카 스파웃에서 들어오는 입력데이터를 모델의 입력에 맞춰 입력하고,
     * 결과 값을 다시 JSON 형태로 변환 후 카프카 볼트로 전송한다.
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        String inputJson = tuple.getString(0);
        String outputJson = null;

        try {
            /* input data를 model의 input에 맞게 변환 */
            instObj = objectMapper.readValue(inputJson, InstObj.class);
            float[][][][] data = instObj.getInstances();

            /* Running session and get output tensor */
            Tensor x = Tensor.create(data);
            Tensor result = sess.runner()
                    .feed("input:0", x)
                    .fetch("output/Softmax:0")
                    .run()
                    .get(0);
            float[][] prob = (float[][]) result.copyTo(new float[1][10]);

            /* 모델의 결과값을 JSON 형태로 변환 */
            predObj.setPredictions(prob);
            ObjectMapper objectMapper = new ObjectMapper();
            outputJson = objectMapper.writeValueAsString(predObj);
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
