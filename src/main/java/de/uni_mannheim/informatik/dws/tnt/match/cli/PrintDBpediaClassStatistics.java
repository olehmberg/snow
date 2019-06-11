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
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.LodCsvTableParser;
import java.io.File;
import java.util.*;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.processing.*;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class PrintDBpediaClassStatistics extends Executable {

    public static void main(String[] args) {
        PrintDBpediaClassStatistics app = new PrintDBpediaClassStatistics();

        if(app.parseCommandLine(PrintDBpediaClassStatistics.class, args)) {
            app.run();
        }
    }

    public void run() {

        for(String cls : getParams()) {
            System.out.println(String.format("Parsing file %s", cls));
            LodCsvTableParser parser = new LodCsvTableParser();
            parser.setParseLists(true);
            parser.setConvertValues(false);
            parser.setUseRowIndexFromFile(false);

            Table t = parser.parseTable(new File(cls));

            // remove all _label columns (they have the same ID as the corresponding object property and will confuse density calculation!)
            Collection<TableColumn> colsToRemove = new LinkedList<>();
            for(TableColumn c : t.getColumns()) {
                if(c.getHeader().endsWith("_label")) {
                    colsToRemove.add(c);
                }
            }
            for(TableColumn c : colsToRemove) {
                t.removeColumn(c);
            }

            Map<TableColumn, Double> density = t.getColumnDensities();
            Map<TableColumn, Double> uniqueness = t.getColumnUniqueness();
            Map<TableColumn, Set<Object>> domains = t.getColumnDomains();

            System.out.println("density   |uniqueness|domain    |name");
            for(TableColumn c : t.getColumns()) {
                System.out.println(String.format("%-10.2f|%-10.2f|%-10d|%s",
                    density.get(c),
                    uniqueness.get(c),
                    domains.get(c).size(),
                    String.format("%s / %s", c.getHeader(), c.getIdentifier())
                ));
            }
        }
        

    }

}