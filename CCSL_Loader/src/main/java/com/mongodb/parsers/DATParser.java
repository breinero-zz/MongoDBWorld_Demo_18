package com.mongodb.parsers;

import javax.management.Descriptor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.NULL;

public class DATParser implements Parser {

    private ArrayList<Token_Descriptor> descriptors;

    private Consumer consumer;

    private Token_Descriptor.PreParser coordinateParser = new Token_Descriptor.PreParser() {
        @Override
        public String preParse(String s) {
            return null;
        }
    };

    public DATParser(Consumer consumer ) {
        this.consumer = consumer;
        descriptors = new ArrayList<>();
        descriptors.add( new Token_Descriptor( "record_type", Token_Descriptor.ColType.String, 0, 2 ));
        descriptors.add( new Token_Descriptor( "station_id", Token_Descriptor.ColType.String,3, 9 ));
        descriptors.add( new Token_Descriptor( "state_code", Token_Descriptor.ColType.String, 3, 5 ) );
        descriptors.add( new Token_Descriptor( "coop_network_id", Token_Descriptor.ColType.String,5, 9 ));
        descriptors.add( new Token_Descriptor( "coop_network_div", Token_Descriptor.ColType.String,9, 11 ) );
        descriptors.add( new Token_Descriptor( "type", Token_Descriptor.ColType.String,11, 15 ) );
        descriptors.add( new Token_Descriptor( "units", Token_Descriptor.ColType.String,15, 17 ) );
        descriptors.add( new Token_Descriptor( "year", Token_Descriptor.ColType.Integer,17, 21 ) );
        descriptors.add( new Token_Descriptor( "month",  Token_Descriptor.ColType.Integer, 21, 23 ) );
        descriptors.add( new Token_Descriptor( "day",  Token_Descriptor.ColType.Integer, 23, 27 ) );
        descriptors.add( new Token_Descriptor( "num_values",  Token_Descriptor.ColType.Integer, 27, 30 ) );
    }

    public void parseLine(String line) {

        Map< String, Token_Descriptor> map =  new HashMap<>();

        int numValues = 0;
        for( Token_Descriptor descriptor : descriptors ) {
            if (descriptor.getName().equals("num_values")){
                numValues = Integer.valueOf(descriptor.parse(line).getValue());
                break;
            }
            map.put( descriptor.getName(), descriptor.parse(line)  );
        }

        // get all the values off the end of the line

        int timeSize = 4;
        int valueSize = 5;
        int position = 30;
        for( int i = 0; i < numValues; i++ ) {

            Token_Descriptor t =
                    new Token_Descriptor( "time",  Token_Descriptor.ColType.Integer, position, (position += timeSize) );
            map.put( t.getName(), t.parse( line ) );
            position += 1;

            t = new Token_Descriptor( "value",  Token_Descriptor.ColType.Integer, position, (position += valueSize) );
            map.put( t.getName(), t.parse( line ) );
            position += 2;

            boolean ignore = false;
            consumer.startEntry();
            for(Map.Entry<String, Token_Descriptor> entry : map.entrySet() ) {
                Token_Descriptor d = entry.getValue();

                try {
                    switch (d.getColType()) {
                        case Integer:
                            int measure = Integer.valueOf(d.getValue());

                            if ( measure == 99999 && d.getName().equals( "value" ) )
                                ignore = true;
                            else
                                consumer.consume(d.getName(), measure );
                            break;
                        case Double:
                            consumer.consume(d.getName(), Double.valueOf( d.getValue() ));
                        case String:
                            consumer.consume(d.getName(),  d.getValue() );
                    }
                } catch ( Exception e ) {
                    System.err.println( e.getClass()+ " on name:"+d.getName()+  ", value: "+ d.getValue()  );
                    throw e;
                }
            }
            if( !ignore )
                consumer.endEntry();
        }
    }

}
