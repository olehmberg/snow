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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.winter.clustering.HierarchicalClusterer.LinkageMode;
import de.uni_mannheim.informatik.dws.winter.clustering.HierarchicalClusterer;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.*;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ClusterSchemata extends Executable {

    @Parameter(names = "-sim")
    private String simLocation;

    @Parameter(names = "-result")
    private String resultLocation;

    @Parameter(names = "-t")
    private double simThreshold = 0.0;

    public static void main(String[] args) throws Exception {
        ClusterSchemata app = new ClusterSchemata();

        if(app.parseCommandLine(ClusterSchemata.class, args)) {
            app.run();
        }
    }

    public void run() throws Exception {

        Collection<Triple<String, String, Double>> data = new LinkedList<>();

        BufferedReader r = new BufferedReader(new FileReader(new File(simLocation)));

        String line = null;

        while((line = r.readLine())!=null) {
            String[] values = line.split(",");

            data.add(new Triple<>(values[0], values[1], Double.parseDouble(values[3])));
        }

        r.close();

        BufferedWriter w = new BufferedWriter(new FileWriter(new File(resultLocation)));

        HierarchicalClusterer<String> clusterer = new HierarchicalClusterer<>(LinkageMode.Max, simThreshold);
        Map<Collection<String>, String> clustering = clusterer.cluster(data);

        int cluId = 0;
        for(Collection<String> clu : clustering.keySet()) {
            double dist = clusterer.getIntraClusterDistance(clu);
            for(String s : clu) {
                w.write(String.format("%s\n", StringUtils.join(new String[] {
                    Integer.toString(cluId),
                    s,
                    Double.toString(-dist)
                }, "\t")));
            }
            cluId++;
        }

        w.close();

    }

}