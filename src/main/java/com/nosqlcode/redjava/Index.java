package com.nosqlcode.redjava;

/**
 * Project: redjava
 * User: thomassilva
 * Date: 8/13/13
 */


public class Index {


    public double scoreStr(String str) {

        double score = 0;

        for (int i = 0; i < str.length(); i++) {

            double multiplier = Math.pow(0.01, i);

            double add;
            switch (str.charAt(i)) {
                case 'a': add = 1; break;
                case 'b': add = 2; break;
                case 'c': add = 3; break;
                case 'd': add = 4; break;
                case 'e': add = 5; break;
                case 'f': add = 6; break;
                case 'g': add = 7; break;
                case 'h': add = 8; break;
                case 'i': add = 9; break;
                case 'j': add = 10; break;
                case 'k': add = 11; break;
                case 'l': add = 12; break;
                case 'm': add = 13; break;
                case 'n': add = 14; break;
                case 'o': add = 15; break;
                case 'p': add = 16; break;
                case 'q': add = 17; break;
                case 'r': add = 18; break;
                case 's': add = 19; break;
                case 't': add = 20; break;
                case 'u': add = 21; break;
                case 'v': add = 22; break;
                case 'w': add = 23; break;
                case 'x': add = 24; break;
                case 'y': add = 25; break;
                case 'z': add = 26; break;
                case '0': add = 27; break;
                case '1': add = 28; break;
                case '2': add = 29; break;
                case '3': add = 30; break;
                case '4': add = 31; break;
                case '5': add = 32; break;
                case '6': add = 33; break;
                case '7': add = 34; break;
                case '8': add = 35; break;
                case '9': add = 36; break;
                default: add = 0;
            }

            score += add * multiplier;
        }

        return score;
    }

}
