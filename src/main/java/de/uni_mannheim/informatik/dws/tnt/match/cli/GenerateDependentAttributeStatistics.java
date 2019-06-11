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
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.processing.*;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class GenerateDependentAttributeStatistics extends Executable {

    @Parameter(names = "-web")
    private String webLocation;

    public static void main(String[] args) throws Exception {
        GenerateDependentAttributeStatistics app = new GenerateDependentAttributeStatistics();

        if(app.parseCommandLine(GenerateDependentAttributeStatistics.class, args)) {
            app.run();
        }
    }

    public void run() throws Exception {

        // load web tables
        File webFile = new File(webLocation);
        WebTables web = WebTables.loadWebTables(webFile, true, true, false, false);
        
        // list all attributes for frequency statistics
        // header, class, key size, type, table name, host
        Collection<Table> relationTables = Q.where(web.getTables().values(), (t)->t.getPath().contains("_rel_"));

        // BufferedWriter w = new BufferedWriter(new FileWriter(new File("dependent_attributes.tsv")));
        BufferedWriter wAll = new BufferedWriter(new FileWriter(new File("attributes.tsv")));
        BufferedWriter wCo = new BufferedWriter(new FileWriter(new File("dependent_attributes_cooc.tsv")));

        for(Table t : relationTables) {
            String url = t.getPath().replaceAll(".+_(.+)_rel_\\d+\\.json", "$1").replace(".json", "");
            Set<TableColumn> key = Q.firstOrDefault(t.getSchema().getCandidateKeys());
            for(TableColumn c : t.getColumns()) {
                wAll.write(String.format("%s\n", StringUtils.join(new String[] {
                    t.getPath(),
                    url,
                    t.getMapping().getMappedClass().getFirst(),
                    c.getHeader(),
                    c.getDataType().toString(),
                    Integer.toString(key.size()),
                    Boolean.toString(key.contains(c)),
                    Boolean.toString(ContextColumns.isContextColumn(c)),
                    Integer.toString(t.getSize())
                }, "\t")));
                if(!key.contains(c)) {
                    // w.write(String.format("%s\n", StringUtils.join(new String[] {
                    //     t.getPath(),
                    //     url,
                    //     t.getMapping().getMappedClass().getFirst(),
                    //     c.getHeader(),
                    //     c.getDataType().toString(),
                    //     Integer.toString(key.size())
                    // }, "\t")));

                    for(TableColumn k : key) {
                        if(!"fk".equalsIgnoreCase(k.getHeader())) {
                            wCo.write(String.format("%s\n", StringUtils.join(new String[] {
                                t.getPath(),
                                url,
                                t.getMapping().getMappedClass().getFirst(),
                                c.getHeader(),
                                c.getDataType().toString(),
                                Boolean.toString(ContextColumns.isContextColumn(c)),
                                Integer.toString(key.size()),
                                k.getHeader(),
                                k.getDataType().toString(),
                                Boolean.toString(ContextColumns.isContextColumn(k)),
                                Integer.toString(t.getSize())
                            }, "\t")));
                        }
                    }
                }
            }
        }

        // w.close();
        wCo.close();
        wAll.close();
    }

}