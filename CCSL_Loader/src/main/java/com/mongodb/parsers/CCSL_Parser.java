package com.mongodb.parsers;

import java.util.ArrayList;

import static java.sql.Types.NULL;

public class CCSL_Parser implements Parser {

    private ArrayList<Token_Descriptor> descriptors;

    private Consumer consumer;

    private Token_Descriptor.PreParser coordinateParser = new Token_Descriptor.PreParser() {
        @Override
        public String preParse(String s) {
            int asterixIndex = s.indexOf( '*' );
            int singleQuoteIndex = s.indexOf( '\'');

            String degrees = s.substring( 0, asterixIndex );
            String minutes = s.substring( asterixIndex + 1, singleQuoteIndex );
            String seconds = s.substring( singleQuoteIndex+1, s.length() );

            return String.valueOf(Double.valueOf(degrees )
                    + ( Double.valueOf( minutes ) / 60 )
                    + ( Double.valueOf( seconds ) / 3600 ));
        }
    };

    public CCSL_Parser ( Consumer consumer ) {
        this.consumer = consumer;
        descriptors = new ArrayList<Token_Descriptor>();
        descriptors.add( new Token_Descriptor( "STATEPROV", Token_Descriptor.ColType.String, 0, 2 ));
        descriptors.add( new Token_Descriptor( "STATION_NAME", Token_Descriptor.ColType.String,3, 33 ));
        descriptors.add( new Token_Descriptor( "COOP_STATION_ID", Token_Descriptor.ColType.String, 34, 40 ) );
        descriptors.add( new Token_Descriptor( "CLIMATE_DIVISION", Token_Descriptor.ColType.Integer,41, 43 ) );
        descriptors.add( new Token_Descriptor( "COUNTY_NAME", Token_Descriptor.ColType.String,44, 73 ));
        descriptors.add( new Token_Descriptor( "NCDC_STATION_ID", Token_Descriptor.ColType.String,75, 82 ) );
        descriptors.add(
                new Token_Descriptor( "LATITUDE", Token_Descriptor.ColType.Double,84, 93, coordinateParser ) );
        descriptors.add(
                new Token_Descriptor( "LONGITUDE", Token_Descriptor.ColType.Double,95, 105, coordinateParser ) );
        descriptors.add( new Token_Descriptor( "ELEVATION", Token_Descriptor.ColType.Double,107, 113 ) );
        descriptors.add( new Token_Descriptor( "END_DATE",  Token_Descriptor.ColType.Integer, 114, 122 ) );
    }

    public void parseLine(String line) {
        for( Token_Descriptor descriptor : descriptors ) {
            String value = descriptor.parse( line ).getValue();

            try {
                switch (descriptor.getColType()) {
                    case Integer:
                        if ( value.isEmpty() )
                            consumer.consume(descriptor.getName(), NULL);
                        else
                            consumer.consume(descriptor.getName(), Integer.valueOf(value));
                        break;
                    case Double:
                        consumer.consume(descriptor.getName(), Double.valueOf(value));
                        break;
                    case String:
                        consumer.consume(descriptor.getName(), value);
                        break;
                }
            } catch ( Exception e ) {
                System.err.println( e.getClass()+ " on "+descriptor.getName()+  ", value: "+value );
            }
        }
    }

}
