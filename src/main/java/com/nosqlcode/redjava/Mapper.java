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
            if (field.isAnnotationPresent(RedStr.class))
                members.add(new Str(field, fieldName));
            else if (field.isAnnotationPresent(RedInt.class))
                members.add(new Int(field, fieldName));
            else if (field.isAnnotationPresent(RedObj.class))
                members.add(new Obj(field, fieldName));
            else if (field.isAnnotationPresent(RedBool.class))
                members.add(new Bool(field, fieldName));


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

        public void save() {
            if (value() != null) {
                pipe.hset(id.getBytes(), attr.getBytes(), format());
                saveIndex();
            }
        }

        abstract protected byte[] format();
        abstract protected double score();

        protected void saveIndex() {
            pipe.zadd(indexKey(), score(), id);
        }
        private String indexKey() {
            return "index:" + type + ":" + attr;
        }
        private void deleteIndex() {
            pipe.zrem(indexKey(), id);
        }
        public void load() {
            future = pipe.hget(id, attr);
        }
        public void delete() {
            pipe.hdel(id, attr);
            deleteIndex();
        }

        protected void sync() {
            if (future.get() != null)
                try {
                    field.set(instance, parse((String) future.get()));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
        }

        abstract protected T parse(String str);

        protected T value() {
            T val = null;
            try {
                val = (T) field.get(instance);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            return val;
        }

        protected void criteria(SearchCriteria searchCriteria) {
            if (value() != null) {
                searchCriteria.addQualifer(indexKey(), score());
            }
        }
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
                super.save();
            }
        }

        @Override
        protected byte[] format() {
            return mapper.getId().getBytes();
        }
        @Override
        protected double score() {
            return index.scoreStr(mapper.getId());
        }

        @Override
        public void sync() {
        }

        @Override
        protected Object parse(String str) {
            return null;
        }

        @Override
        public void load() {
            if (mapper != null) {
                super.load();
                mapper.load();
            }
        }

        @Override
        public void delete() {
            super.delete();
            if (mapper != null) {
                mapper.delete();
            }
        }
    }

    public class Str extends Member<String> {

        public Str(Field field, String attr) {
            super(field, attr);
        }

        @Override
        protected byte[] format() {
            return value().getBytes();
        }
        @Override
        protected double score() {
            return index.scoreStr(value());
        }
        @Override
        protected String parse(String str) {
            return str;
        }
    }

    public class Int extends Member<Integer> {

        public Int(Field field, String attr) {
            super(field, attr);
        }

        @Override
        protected byte[] format() {
            return new byte[]{value().byteValue()};
        }
        @Override
        protected double score() {
            return value();
        }
        @Override
        protected Integer parse(String str) {
            return Integer.parseInt(str);
        }
    }

    public class Bool extends Member<Boolean> {

        public Bool(Field field, String attr) {
            super(field, attr);
        }

        @Override
        protected byte[] format() {
            return new byte[]{toRed(value()).byteValue()};
        }
        @Override
        protected double score() {
            return toRed(value());
        }
        @Override
        protected Boolean parse(String str) {
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
