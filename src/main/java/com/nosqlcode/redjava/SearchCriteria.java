package com.nosqlcode.redjava;

import java.util.ArrayList;

/**
 * Created by thomas on 4/5/14.
 */

public class SearchCriteria {

    private ArrayList<Qualifer> qualifers = new ArrayList<>();

    public class Qualifer {
        public String indexKey;
        public Double score;

        Qualifer(String indexKey, Double score) {
            this.indexKey = indexKey;
            this.score = score;
        }

        public void print() {
            System.out.println(indexKey + "\t" + score);
        }
    }

    public void addQualifer(String indexKey, double score) {
        qualifers.add(new Qualifer(indexKey, score));
    }

    public ArrayList<Qualifer> getQualifiers() {
        return qualifers;
    }
}
