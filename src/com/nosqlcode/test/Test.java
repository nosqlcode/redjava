package com.nosqlcode.test;

import com.nosqlcode.redjava.Mapper;
import com.nosqlcode.redjava.Pool;

/**
 * User: thomas
 * Date: 10/10/13
 * Time: 8:48 AM
 */

public class Test {


    public static void main(String[] args) {


        Pool.connect("127.0.0.1", 6379);


        Customer thomas = new Customer("thomas", "silva");
        thomas.address = new Address("123 fake street", "a city", "89764", "AA");

        Mapper mapper = new Mapper(thomas);
        mapper.save();


        Customer tom = new Customer();
        Mapper mapper2 = new Mapper(tom, mapper.getId());
        mapper2.load();

        System.out.println(tom.firstName + " " + tom.lastName);
    }
}
