package com.he.datastream;

import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;
import com.he.datastream.KafkaHelper.*;

public class MyAggregateFun implements AggregateFunction<Test2,MyAccumulator,MyAccumulator> {

    @Override
    public MyAccumulator createAccumulator() {
        return new MyAccumulator();
    }

    @Override
    public MyAccumulator add(Test2 value, MyAccumulator accumulator) {
        accumulator.setType(value.orderType());
        accumulator.addId(value.id());
        return accumulator;
    }

    @Override
    public MyAccumulator getResult(MyAccumulator accumulator) {
//        System.out.println("getResult:"+accumulator);
        return accumulator;
    }

    @Override
    public MyAccumulator merge(MyAccumulator a, MyAccumulator b) {
//        System.out.println("merge:"+a);
//        System.out.println("merge:"+b);
        return new MyAccumulator();
    }
}

class MyAccumulator{
    private String type;
    private List<String> ids;

    public MyAccumulator() {
        type = "";
        ids = new ArrayList<>();
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getIds() {
        return ids;
    }

    public void setIds(List<String> ids) {
        this.ids = ids;
    }

    public void addId(String id){
        this.ids.add(id);
    }

    @Override
    public String toString() {
        return "MyAccumulator{" +
                "type='" + type + '\'' +
                ", ids=" + ids +
                '}';
    }
}