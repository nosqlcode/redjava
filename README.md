##redjava
a redis mapper for java - (model, save, and load)
(work in progress)
currently this library creates indexes with sorted sets,
however finding objects is functionality to be added in time...


##Prequisites
jedis client - [jar](https://github.com/xetorthio/jedis/archive/master.zip)
commons-pool - [jar](http://apache.mirrors.tds.net//commons/pool/binaries/commons-pool-1.6-bin.zip)


##Import
```java
import com.nosqlcode.redjava.Mapper;
import com.nosqlcode.redjava.Pool;
```


##Start
```java
Pool.connect("127.0.0.1", 6379);
```


##Model
```java
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
```


##Save
```java
Customer thomas = new Customer("thomas", "silva");
thomas.address = new Address("123 fake street", "a city", "89764", "AA");

Mapper mapper = new Mapper(thomas);
mapper.save();
```


#Load
```java
Customer tom = new Customer();
Mapper mapper2 = new Mapper(tom, mapper.getId());
mapper2.load();

System.out.println(tom.firstName + " " + tom.lastName);
```