package com.nosqlcode.test;

import com.nosqlcode.redjava.RedStr;

/**
 * User: thomas
 * Date: 10/10/13
 * Time: 9:20 AM
 */

public class Address {


    @RedStr
    public String street, city, zip, state;


    public Address() {
    }

    public Address(String street, String city, String zip, String state) {
        this.street = street;
        this.city = city;
        this.zip = zip;
        this.state = state;
    }
}
