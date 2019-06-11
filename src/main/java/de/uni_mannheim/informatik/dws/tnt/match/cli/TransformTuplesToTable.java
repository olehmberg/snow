/*
 * Copyright (c) 2017 Data and Web Science Group, University of Mannheim, Germany (http://dws.informatik.uni-mannheim.de/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package de.uni_mannheim.informatik.dws.tnt.match.cli;

import de.uni_mannheim.informatik.dws.winter.utils.Executable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.RecordCSVFormatter;
import de.uni_mannheim.informatik.dws.winter.model.io.CSVDataSetFormatter;
import de.uni_mannheim.informatik.dws.winter.processing.*;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TransformTuplesToTable extends Executable {

    // @Parameter(names = "-tuples", required=true)
    // private String tuplesLocation;

    public static void main(String[] args) throws IOException {
        TransformTuplesToTable app = new TransformTuplesToTable();

        if(app.parseCommandLine(TransformTuplesToTable.class, args)) {
            app.run();
        }
    }

    public void run() throws IOException {
        for(String s : this.getParams()) {
            System.out.println(String.format("Converting %s", s));
            tuplesToTable(s);
        }
    }

    public void tuplesToTable(String s) throws IOException {
        BufferedReader r = new BufferedReader(new FileReader(s));
        String line = null;

        DataSet<Record, Attribute> ds = new HashedDataSet<>();
        Attribute dependant = null;
        Attribute source = new Attribute("__source");
        source.setName("Source");
        ds.addAttribute(source);
        Attribute attributeName = new Attribute("AttributeName");
        attributeName.setName("Attribute Name");
        ds.addAttribute(attributeName);

        int idx = 0;
        while((line = r.readLine())!=null) {
            String[] values = line.split("\t");

            if(values.length>6) {
                String att = values[4];
                String key = values[5];
                String value = values[6];

                if(dependant==null) {
                    dependant = new Attribute("__dependant");
                    dependant.setName(att);
                    ds.addAttribute(dependant);
                }

                Record record = new Record(Integer.toString(idx++));

                record.setValue(source, values[0]);
                record.setValue(dependant, value);
                record.setValue(attributeName, att);

                try {
                    // JsonReader jr = new JsonReader(new StringReader(key));
                    // jr.setLenient(true);
                    // jr.beginObject();
                    // while(jr.hasNext()) {
                    //     String name = jr.nextName();
                    //     Attribute a = ds.getSchema().getRecord(name);
                    //     if(a==null) {
                    //         a = new Attribute(name);
                    //         a.setName(name);
                    //     }

                    //     record.setValue(a, jr.nextString());
                    // }
                    // jr.endObject();
                    // jr.close();
                    key = key.substring(1, key.length()-1);
                    String[] parts = key.split("\",");
                    for(String part : parts) {
                        String[] kv = part.split(": \"");
                        Attribute a = ds.getSchema().getRecord(kv[0].trim());
                        if(a==null) {
                            a = new Attribute(kv[0].trim());
                            a.setName(kv[0].trim());
                            ds.addAttribute(a);
                        }

                        if(kv.length>1) {
                            if(kv[1].endsWith("\"")) {
                                kv[1] = kv[1].substring(0, kv[1].length()-1);
                            }
                            record.setValue(a, kv[1]);
                        }
                    }
                } catch(Exception e) { 
                    System.err.println(key);
                    e.printStackTrace(); 
                }

                ds.add(record);
            }
        }

        r.close();

        System.out.println(String.format("%s: writing results to %s", s, new File(s + ".table.csv").getAbsolutePath()));
        new RecordCSVFormatter().writeCSV(new File(s + ".table.csv"), ds, new ArrayList<>(ds.getSchema().get()));
    }

}