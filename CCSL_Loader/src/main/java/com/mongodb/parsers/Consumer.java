package com.mongodb.parsers;

public interface Consumer {
    void consume(String name, Integer value );
    void consume(String name, Double value );
    void consume(String name, String value );

    void startEntry();
    void endEntry();
}