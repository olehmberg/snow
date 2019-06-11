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
package de.uni_mannheim.informatik.dws.snow;

import java.util.*;
import org.apache.commons.lang3.time.DurationFormatUtils;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SnowRuntime {

    private List<SnowRuntime> partialRuntimes;
    private long start;
    private long end = -1;
    private String name;

    public SnowRuntime(String name) {
        this.name = name;
    }

    public void startMeasurement() {
        partialRuntimes = new LinkedList<>();
        start = System.currentTimeMillis();
    }

    public SnowRuntime startMeasurement(String component) {
        SnowRuntime r = new SnowRuntime(component);
        r.startMeasurement();
        partialRuntimes.add(r);
        return r;
    }

    public void endMeasurement() {
        end = System.currentTimeMillis();
    }

    public void print() {
        System.out.println(String.format("*** Runtime report for '%s'", name));
        print("");
    }

    public long getDuration() {
        if(end==-1) {
            return System.currentTimeMillis() - start;
        } else {
            return end - start;
        }
    }

    protected void print(String prefix) {
        System.out.println(String.format("%s%s\t%s",            
            prefix,
            DurationFormatUtils.formatDuration(getDuration(), "HH:mm:ss.S"),
            name
        ));
        for(SnowRuntime r : partialRuntimes) {
            r.print(prefix + "|\t");
        }
    }
}