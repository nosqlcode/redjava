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

    public void close() {
        Pool.returnJedis(jedis);
    }


    // abstract class for all member types to extend
    private abstract class Member {

        protected Field field;
        protected String attr;
        protected Response future;

        Member(Field field, String attr) {
            this.field = field;
            this.attr = attr;
        }

        abstract void save();
        abstract public void sync();

        protected void saveIndex(double score) {
            pipe.zadd("index:" + type + ":" + attr, score, id);
        }
        private void deleteIndex() {
            pipe.zrem("index:" + type + ":" + attr, id);
        }
        public void load() {
            future = pipe.hget(id, attr);
        }
        public void delete() {
            pipe.hdel(id, attr);
            deleteIndex();
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
                pipe.hset(id, attr, mapper.getId());
                saveIndex(index.scoreStr(mapper.getId()));
            }
        }

        @Override
        public void sync() {
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
            mapper.delete();
        }
    }

    public class Str extends Member {


        public Str(Field field, String attr) {
            super(field, attr);
        }

        @Override
        public void save() {
            if (value() != null) {
                pipe.hset(id, attr, value());
                saveIndex(index.scoreStr(value()));
            }
        }
        @Override
        public void sync() {
            if (future.get() != null)
                try {
                    field.set(instance, future.get());
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
        }

        private String value() {
            String val = null;
            try {
                val = (String) field.get(instance);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            return val;
        }
    }

    public class Int extends Member {

        public Int(Field field, String attr) {
            super(field, attr);
        }

        @Override
        public void save() {
            if (value() == null)
                return;
            pipe.hset(id, attr, Integer.toString(value()));
            saveIndex(value());
        }
        @Override
        public void sync() {
            if (future.get() != null)
                try {
                    field.set(instance, Integer.parseInt((String) future.get()));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
        }

        private Integer value() {
            Integer val = null;
            try {
                val = (Integer) field.get(instance);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            return val;
        }
    }

    public class Bool extends Member {

        public Bool(Field field, String attr) {
            super(field, attr);
        }

        @Override
        public void save() {
            if (value() == null)
                return;
            pipe.hset(id, attr, toRed(value()).toString());
            saveIndex(toRed(value()));
        }
        @Override
        public void sync() {
            if (future.get() != null) {
                try {
                    field.set(instance, fromRed((String) future.get()));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
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

        private Boolean value() {
            Boolean val = null;
            try {
                val = (Boolean) field.get(instance);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            return val;
        }
    }


}
