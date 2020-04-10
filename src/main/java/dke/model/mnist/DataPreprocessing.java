package dke.model.mnist;

public class DataPreprocessing {
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
