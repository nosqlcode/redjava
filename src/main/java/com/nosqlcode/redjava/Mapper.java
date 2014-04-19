package com.nosqlcode.redjava;

/**
 * Project: redjava
 * User: thomassilva
 * Date: 8/13/13
 */

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;


// demo comment
// additional comment

public class Mapper {


    // reference to original object
    private Object instance;

    // members that access original object members
    private ArrayList<Member> members = new ArrayList<>();
    private String type;
    private String id;

    // pool of redis client connections
    private Jedis jedis = Pool.getJedis();
    private Pipeline pipe;

    private Index index = new Index();


    public Mapper(Object obj) {

        instance = obj;
        type = instance.getClass().getSimpleName();


        // iterate fields of object
        for (Field field : obj.getClass().getFields()) {


            /* obtain reference variable name to later be used
            as the attribute in a redis hashmap */
            String fieldName = field.getName();


            /* determine what type of member the field is
            and create member */


            if (field.isAnnotationPresent(RedLst.class)) {

                if (field.isAnnotationPresent(RedStr.class))
                    members.add(new PrimitiveListMember<>(
                            field, fieldName, new StringFormatter()));
                else if (field.isAnnotationPresent(RedInt.class))
                    members.add(new PrimitiveListMember<>(
                            field, fieldName, new IntegerFormatter()));
                else if (field.isAnnotationPresent(RedBool.class))
                    members.add(new PrimitiveListMember<>(
                            field, fieldName, new BooleanFormatter()));
            } else {

                if (field.isAnnotationPresent(RedStr.class))
                    members.add(new PrimitiveMember<>(
                            field, fieldName, new StringFormatter()));
                else if (field.isAnnotationPresent(RedInt.class))
                    members.add(new PrimitiveMember<>(
                            field, fieldName, new IntegerFormatter()));
                else if (field.isAnnotationPresent(RedObj.class))
                    members.add(new Obj(field, fieldName));
                else if (field.isAnnotationPresent(RedBool.class))
                    members.add(new PrimitiveMember<>(
                            field, fieldName, new BooleanFormatter()));
            }

        }
    }


    // construct new mapper object with a provided id
    public Mapper(Object obj, String id) {

        this(obj);
        this.id = id;
    }


    public String getId() {
        return id;
    }


    // persist data to redis
    public void save() {

        // create new hash map if there is no id
        if (id == null)
            id = type + ":" + Long.toString(jedis.incr(type));

        pipe = jedis.pipelined();
        for (Member member : members) {
            member.save();
        }
        pipe.sync();
    }

    // pipeline redis get requests and then initialize them into object instance
    public void load() {

        pipe = jedis.pipelined();
        for (Member member : members) {
            member.load();
        }
        pipe.sync();
        for (Member member : members) {
            member.sync();
        }
    }

    public void load(String id) {

        this.id = id;
        this.load();
    }

    public void delete() {

        pipe = jedis.pipelined();
        for (Member member : members) {
            member.delete();
        }
        pipe.sync();
    }

    public SearchCriteria getCriteria() {

        SearchCriteria searchCriteria = new SearchCriteria();

        for (Member member : members) {
            member.criteria(searchCriteria);
        }

        return  searchCriteria;
    }

    public void close() {
        Pool.returnJedis(jedis);
    }


    // abstract class for all member types to extend
    private abstract class Member<T> {

        protected Field field;
        protected String attr;
        protected Response future;

        Member(Field field, String attr) {
            this.field = field;
            this.attr = attr;
        }

        abstract public void save();
        abstract public void load();
        public abstract void sync();
        abstract public void delete();

        protected void saveIndex(double score) {
            pipe.zadd(indexKey(), score, id);
        }
        protected String indexKey() {
            return "index:" + type + ":" + attr;
        }
        protected void deleteIndex() {
            pipe.zrem(indexKey(), id);
        }

        protected T value() {
            T val = null;
            try {
                val = (T) field.get(instance);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            return val;
        }

        abstract public void criteria(SearchCriteria searchCriteria);
    }

    public class Obj extends Member {

        private Mapper mapper;

        public Obj(Field field, String attr) {

            super(field, attr);

            Object target = null;
            try {
                target = field.get(instance);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            if (target != null)
                mapper = new Mapper(target);
        }

        @Override
        public void save() {
            if (mapper != null) {
                mapper.save();
                pipe.hset(id.getBytes(), attr.getBytes(),
                        mapper.getId().getBytes());
            }
        }

        @Override
        public void load() {
            if (mapper != null) {
                future = pipe.hget(id.getBytes(), attr.getBytes());
            }
        }

        @Override
        public void sync() {
            String value = (String) value();
            if (value != null) {
                mapper.load(value);
            }
        }

        @Override
        public void delete() {
            if (mapper != null) {
                pipe.hdel(id, attr);
                mapper.delete();
            }
        }

        @Override
        public void criteria(SearchCriteria searchCriteria) {
        }
    }

    private interface PrimitiveFormatter<T> {
        byte[] format(T value);
        double score(T value);
        T parse(String str);
    }

    private class IntegerFormatter implements PrimitiveFormatter<Integer> {
        @Override
        public byte[] format(Integer value) {
            return new byte[]{value.byteValue()};
        }

        @Override
        public double score(Integer value) {
            return value;
        }

        @Override
        public Integer parse(String str) {
            return Integer.parseInt(str);
        }
    }

    private class StringFormatter implements PrimitiveFormatter<String> {
        @Override
        public byte[] format(String value) {
            return value.getBytes();
        }

        @Override
        public double score(String value) {
            return index.scoreStr(value);
        }

        @Override
        public String parse(String str) {
            return str;
        }
    }

    private class BooleanFormatter implements PrimitiveFormatter<Boolean> {
        @Override
        public byte[] format(Boolean value) {
            return new byte[]{toRed(value).byteValue()};
        }

        @Override
        public double score(Boolean value) {
            return toRed(value);
        }

        @Override
        public Boolean parse(String str) {
            return fromRed(str);
        }

        private Integer toRed(Boolean val) {
            if (val)
                return 1;
            else
                return 0;
        }
        private Boolean fromRed(String val) {
            int num = Integer.parseInt(val);
            if (num == 1)
                return true;
            else
                return false;
        }
    }

    private class PrimitiveMember<T> extends Member<T> {

        PrimitiveFormatter<T> primitiveFormatter;
        public PrimitiveMember(Field field, String attr, PrimitiveFormatter<T> primitiveFormatter)
        {
            super(field, attr);
            this.primitiveFormatter = primitiveFormatter;
        }

        @Override
        public void save() {
            T t = value();
            if (t != null) {
                pipe.hset(id.getBytes(), attr.getBytes(),
                        primitiveFormatter.format(t));
                saveIndex(primitiveFormatter.score(t));
            }
        }

        @Override
        public void load() {
            future = pipe.hget(id, attr);
        }

        @Override
        public void sync() {
            String temp = (String) future.get();
            if (temp != null)
                try {
                    field.set(instance, primitiveFormatter.parse(temp));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
        }

        @Override
        public void delete() {
            pipe.hdel(id, attr);
            deleteIndex();
        }

        @Override
        public void criteria(SearchCriteria searchCriteria) {
            T t = value();
            if (t != null) {
                searchCriteria.addQualifer(indexKey(), primitiveFormatter.score(t));
            }
        }
    }

    private class PrimitiveListMember<T> extends Member<List<T>> {

        String memberId;
        Response<String> memberIdFuture;

        PrimitiveFormatter<T> primitiveFormatter;
        public PrimitiveListMember(Field field, String attr, PrimitiveFormatter<T> primitiveFormatter)
        {
            super(field, attr);
            this.primitiveFormatter = primitiveFormatter;
        }

        private String subType() {
            return type + ":" + attr;
        }

        @Override
        public void save() {
            List<T> value = value();
            if (value != null) {

                // get new member id if there is none
                if (memberId == null) {
                    memberId = subType() + ":" + Long.toString(jedis.incr(subType()));
                } else {
                    // remove pre existing list
                    pipe.del(memberId);
                    deleteIndex();
                }

                // save reference to list
                pipe.hset(id.getBytes(), attr.getBytes(), memberId.getBytes());

                // save the new list
                for (T t: value) {
                    pipe.lpush(memberId.getBytes(), primitiveFormatter.format(t));
                    saveIndex(primitiveFormatter.score(t));
                }
            }
        }

        @Override
        public void load() {
            memberIdFuture = pipe.hget(id, attr);
        }

        @Override
        public void sync() {
            memberId = memberIdFuture.get();

            if (memberId != null) {

                List<String> temp = jedis.lrange(memberId, 0, -1);

                if (temp != null) {
                    List<T> tempConverted = new ArrayList<>();
                    for (String str: temp) {
                        tempConverted.add(primitiveFormatter.parse(str));
                    }
                    try {
                        field.set(instance, tempConverted);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        @Override
        public void delete() {
            pipe.hdel(id, attr);
            pipe.del(memberId);
            deleteIndex();
        }

        @Override
        public void criteria(SearchCriteria searchCriteria) {
            List<T> value = value();
            if (value != null) {
                for (T t: value) {
                    searchCriteria.addQualifer(indexKey(), primitiveFormatter.score(t));
                }
            }
        }
    }

    public abstract static class Finder<T> {

        private Jedis jedis = Pool.getJedis();
        private SearchCriteria searchCriteria;

        public Finder(SearchCriteria searchCriteria) {
            this.searchCriteria = searchCriteria;
        }

        public abstract T newInstance();

        private Set<String> matchindIds(SearchCriteria.Qualifer qualifer) {
            return jedis.zrangeByScore(qualifer.indexKey, qualifer.score, qualifer.score);
        }

        private ArrayList<String> retrieveIds() {
            ArrayList<String> ids = new ArrayList<>();

            HashMap<String, Integer> idOccurances = new HashMap<>();


            for(SearchCriteria.Qualifer qualifer: searchCriteria.getQualifiers()) {

                for (String id: matchindIds(qualifer)) {
                    int previousOccurances = 0;
                    if (idOccurances.containsKey(id)) {
                        previousOccurances = idOccurances.get(id);
                    }
                    idOccurances.put(id, ++previousOccurances);
                }
            }

            int numOfQualifers = searchCriteria.getQualifiers().size();

            idOccurances.forEach(new BiConsumer<String, Integer>() {
                @Override
                public void accept(String s, Integer integer) {
                    if (integer == numOfQualifers) {
                        ids.add(s);
                    }
                }
            });

            return ids;
        }

        private T load(String id) {
            T t = newInstance();
            Mapper mapper = new Mapper(t, id);
            mapper.load();
            return t;
        }

        public ArrayList<T> find() {
            ArrayList<T> tArrayList = new ArrayList<>();
            for (String id: retrieveIds()) {
                tArrayList.add(load(id));
            }
            return tArrayList;
        }
    }

}
