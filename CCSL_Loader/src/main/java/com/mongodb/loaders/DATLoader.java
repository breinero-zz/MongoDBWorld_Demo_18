package com.mongodb.loaders;

import com.mongodb.MongoClient;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.parsers.CCSL_Parser;
import com.mongodb.parsers.Consumer;
import com.mongodb.parsers.DATParser;
import com.mongodb.parsers.Parser;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class DATLoader implements Consumer {

    private Document doc;
    private MongoCollection collection;

    @Override
    public void consume(String name, Integer value) {
        doc.append( name, value );
    }

    @Override
    public void consume(String name, Double value) {
        doc.append( name, value );
    }

    @Override
    public void consume(String name, String value) {
        doc.append( name, value );
    }

    @Override
    public void startEntry() {
        doc = new Document();
    }

    @Override
    public void endEntry() {
        collection.insertOne( doc );
        /*try {
            collection.insertOne( doc );
        } catch ( MongoWriteException mre ) {
            Bson filter = Filters.eq("_id", _id);
            UpdateOptions options = new UpdateOptions().upsert(true);
            doc.remove( "_id" );
            collection.replaceOne(filter, doc, options );
        }
        */
    }

    public DATLoader(String filename ) throws IOException {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("noaa");
        collection = database.getCollection("log");

        File file = new File( filename );
        BufferedReader br = new BufferedReader(new FileReader(file));

        Parser parser = new DATParser( this );



        String line;
        while (( line  = br.readLine()) != null)
            parser.parseLine( line );

        mongoClient.close();
    }

    public static void main( String[] args ) {
        try {
            new DATLoader(args[0]);
        } catch( Exception e ) {
            e.printStackTrace();
        }
    }
}
