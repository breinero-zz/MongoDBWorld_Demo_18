package com.mongodb.parsers;

public class Token_Descriptor {

    private String name;
    private String value;
    private Integer start;
    private Integer end;
    private ColType colType;
    private PreParser preParser = null;

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public enum ColType {String, Integer, Double}

    public Token_Descriptor parse(String in) {
        try {
            value = in.substring(start, end).trim();
            if (preParser != null)
                 value = preParser.preParse(value);
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(
                    e.getMessage() + " token: " + this.getName()
                            + ", start: " + start
                            + ", end: " + end
            );
            throw e;
        }
        return this;
    }

    public ColType getColType() {
        return colType;
    }

    public Token_Descriptor(String name, ColType colType, Integer start, Integer end) {
        this.name = name;
        this.colType = colType;
        this.start = start;
        this.end = end;
    }

    public Token_Descriptor(String name, ColType colType, Integer start, Integer end, PreParser preParser) {
        this.name = name;
        this.colType = colType;
        this.start = start;
        this.end = end;
        this.preParser = preParser;
    }

    public abstract static class PreParser {
        public abstract String preParse(String s);
    }
}
