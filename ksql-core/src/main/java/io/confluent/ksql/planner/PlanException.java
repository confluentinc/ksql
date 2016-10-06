package io.confluent.ksql.planner;


public class PlanException extends  RuntimeException {

    public PlanException(String message) {
        super(message);
    }

}
