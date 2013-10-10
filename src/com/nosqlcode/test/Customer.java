package com.nosqlcode.test;

import com.nosqlcode.redjava.RedObj;
import com.nosqlcode.redjava.RedStr;

/**
 * User: thomas
 * Date: 10/10/13
 * Time: 8:44 AM
 */

public class Customer {


    @RedStr
    public String firstName, lastName;

    @RedObj
    public Address address;


    public Customer() {
    }

    public Customer(String first, String last) {

        firstName = first;
        lastName = last;
    }

}
