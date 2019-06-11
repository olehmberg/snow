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
import de.uni_mannheim.informatik.dws.winter.utils.ProgressReporter;
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

import com.beust.jcommander.Parameter;

import org.apache.commons.lang.time.DurationFormatUtils;

import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.processing.*;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SelectExtractedTuples extends Executable {

    @Parameter(names = "-filter", required=true)
    private String filterLocation;

    @Parameter(names = "-data", required=true)
    private String dataLocation;

    public static void main(String[] args) throws IOException {
        SelectExtractedTuples app = new SelectExtractedTuples();

        if(app.parseCommandLine(SelectExtractedTuples.class, args)) {
            app.run();
        }
    }

    public void run() throws IOException {

        BufferedReader r = new BufferedReader(new FileReader(new File(filterLocation)));

        System.err.println(String.format("Reading filters from %s", filterLocation));
        Map<String, Set<String>> filter = new HashMap<>();
        Map<String, String> outputAssignment = new HashMap<>();
        String line = null;
        while((line = r.readLine()) != null) {
            String[] values = line.split("\t");
            if(values!=null && values.length>2) {
                Set<String> names = filter.get(values[0]);
                if(names==null) {
                    names = new HashSet<>();
                    filter.put(values[0], names);
                }
                for(int i = 2; i < values.length; i++) {
                    names.add(values[i]);
                    System.err.println(String.format("%s: %s", values[0], values[i]));
                    outputAssignment.put(values[0] + values[i], values[1]);
                }  
                
            }
        }

        r.close();

        System.err.println(String.format("Reading data from %s", dataLocation));

        Map<String, BufferedWriter> writers = new HashMap<>();
        for(String key : outputAssignment.values()) {
            BufferedWriter w = new BufferedWriter(new FileWriter("selected_tuples_" + key + ".tsv"));
            writers.put(key, w);
        }

		start = LocalDateTime.now();
        lastTime = start;
        long done = 0;
        long matches = 0;
        r = new BufferedReader(new FileReader(new File(dataLocation)));
        while((line = r.readLine()) != null) {
            String[] values = line.split("\t");
            if(values!=null && values.length>4) {
                String file = values[0];
                String att = values[4];
                String outputKey = outputAssignment.get(file + att);
                Set<String> names = filter.get(file);
                if(names!=null) {
                    if(names.contains(att)) {
                        System.out.println(line);
                        matches++;

                        writers.get(outputKey).write(line);
                        writers.get(outputKey).write("\n");
                    }
                }
            }
            done++;
            report(done, matches);
        }

        r.close();

        for(BufferedWriter w : writers.values()) {
            w.close();
        }

        System.err.println("done.");
    }

    LocalDateTime start;
    LocalDateTime lastTime;
	public void report(long done, long matches) {
		// report status every second
		LocalDateTime now = LocalDateTime.now();
        long durationSoFar = Duration.between(start, now).toMillis();
        long between = (Duration.between(lastTime, now).toMillis());
        
		if (between > 1000) {
			System.err.println(String.format(
					"%,d lines with %,d matches processed after %s",
					done, matches,
					DurationFormatUtils.formatDurationHMS(durationSoFar)));
			lastTime = now;
		}
	}

}