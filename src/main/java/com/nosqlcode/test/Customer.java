package com.nosqlcode.test;

import com.nosqlcode.redjava.RedLst;
import com.nosqlcode.redjava.RedObj;
import com.nosqlcode.redjava.RedStr;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * User: thomas
 * Date: 10/10/13
 * Time: 8:44 AM
 */

public class Customer {


    @RedStr
    public String firstName, lastName;

    @RedLst
    @RedStr
    public ArrayList<String> knickNames;

    @RedObj
    public Address address;


    public Customer() {
    }

    public Customer(String first, String last, String ... names) {

        firstName = first;
        lastName = last;

        knickNames = new ArrayList<>(Arrays.asList(names));
    }

}
