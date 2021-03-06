/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
    https://examples.javacodegeeks.com/core-java/json/java-json-parser-example/
    simple=json (need dependency in pom.xml): http://www.tutorialspoint.com/json/json_java_example.htm
 */
package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

import java.util.Iterator;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * In this example, we implement a simple LineSplit program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text;
 * the code split each text line in string into words and then write back into a sink topic "streams-linesplit-output" where
 * each record represents a single word.
 */
public class LineSplit {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.kafka-cluster-shared.stg1.walmart.com:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        String payload = "[{\"batchId\": \"c0212936-029d-41df-b0fb-911ef1fa8789\"}, {\"correlationId\": \"6164156a-aa32-4a96-aaba-8546bab47fcd\"}, {\"stores\" : [ 1, 2, 3, 4 ]}, {\"offer\": [{\"isNeverOutInd\" : \"X\"}, {\"second\":\"thing2\"}] }, {\"1\":{\"2\":{\"3\":{\"4\":[5,{\"6\":7}]}}}}]";

        builder.<String, String>stream("streams-plaintext-input")
                .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                     @Override
                     public Iterable<String> apply(String value) {
                         String parsedValue = parsePayload3(value);
                         return Arrays.asList("original input value: " + value, "new parsed value: " + parsedValue);
                     }
                 })
               .to("streams-wordcount-output");

        /* ------- use the code below for Java 8 and uncomment the above ----

        builder.stream("streams-plaintext-input")
               .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
               .to("streams-linesplit-output");

           ----------------------------------------------------------------- */


        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public static String parsePayload3(String payload){

      try {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonPayload = (JSONObject) jsonParser.parse(payload);
        String parsedPayload = "";

        parsedPayload += "\nraw json object: " + jsonPayload + "\n\n";
        parsedPayload += "batchId is: " + (String) jsonPayload.get("batchId") + "\n";

        JSONObject contentObj = (JSONObject) jsonPayload.get("content");
        parsedPayload += "recordType inside content is: " + contentObj.get("recordType") + "\n";

        JSONObject pricingObj = (JSONObject) contentObj.get("Pricing");
        JSONArray pricePointsArr = (JSONArray) pricingObj.get("pricePoints");
        Iterator pricePointsIterator = pricePointsArr.iterator();

        // take each value from the json array separately
        while (pricePointsIterator.hasNext()) {
          JSONObject innerObj = (JSONObject) pricePointsIterator.next();
          parsedPayload += "retail: " + innerObj.get("retail") + "\n";
        }

        return parsedPayload + "\n";
        
      } catch (ParseException ex) {
        ex.printStackTrace();
      } catch (NullPointerException ex) {
        ex.printStackTrace();
      }

      return "error: reached end of parsePayLoad3() without returning anything.";
    }

    public static String parsePayload2(String payload){

      try {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(payload);

        System.out.println("jsonObject is: " + jsonObject);

        // get a String from the JSON object
        String firstName = (String) jsonObject.get("firstname");
        System.out.println("The first name is: " + firstName);

        // get a number from the JSON object
        long id =  (long) jsonObject.get("id");
        System.out.println("The id is: " + id);

        // get an array from the JSON object
        JSONArray lang = (JSONArray) jsonObject.get("languages");

        // take the elements of the json array
        for(int i=0; i<lang.size(); i++){
          System.out.println("The " + i + " element of the array: " + lang.get(i));
        }
        Iterator i = lang.iterator();

        // take each value from the json array separately
        while (i.hasNext()) {
          JSONObject innerObj = (JSONObject) i.next();
          System.out.println("language "+ innerObj.get("lang") +
          " with level " + innerObj.get("knowledge"));
        }
        // handle a structure into the json object
        JSONObject structure = (JSONObject) jsonObject.get("job");
        System.out.println("Into job structure, name: " + structure.get("name"));

      } catch (ParseException ex) {
        ex.printStackTrace();
      } catch (NullPointerException ex) {
        ex.printStackTrace();
      }

      return "error: reached end of parsePayLoad2() without returning anything.";
    }

    //JSONObject used as map/key system, where you can get item via its object key name. JSONArray useful for iterating or getting by position.
    public static String parsePayload(String payload){
      JSONParser parser = new JSONParser();
      String returnString = "";

      try{
         Object payObj = parser.parse(payload);
         JSONArray payArray = (JSONArray)payObj;

         for(int i = 0; i < payArray.size(); i++)
           returnString += payArray.get(i) + "\n";

         return returnString;
      }
      catch(ParseException pe){
         System.out.println("position: " + pe.getPosition());
         System.out.println(pe);
      }

      return "error: reached end of parsePayLoad() without returning anything.";
    }


}
