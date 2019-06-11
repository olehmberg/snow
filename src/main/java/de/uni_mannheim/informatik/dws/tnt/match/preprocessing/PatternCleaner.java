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
package de.uni_mannheim.informatik.dws.tnt.match.preprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.beust.jcommander.Parameter;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharSequenceNodeFactory;
import com.googlecode.concurrenttrees.solver.LCSubstringSolver;

import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * Detects patterns in strings and removes tokens that belong to the pattern,
 * while keeping all non-pattern values
 * 
 * Example: washington and lee at. univ. of the south | box score | 9/11/2010
 * kean university at. the college of nj | box score | 10/20/2012 st. lawrence
 * at. hobart | box score | 10/1/2011
 * 
 * is normalised to: washington and lee univ. of the south 9/11/2010 kean
 * university the college of nj 10/20/2012 st. lawrence hobart 10/1/2011
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class PatternCleaner extends Executable {

    @Parameter(names = "-tbl")
    private String tableLocation;

    @Parameter(names = "-minSup", description = "min percentage of unique values that share a token to be considered as a pattern cluster")
    private double minSupport = 0.05;

    @Parameter(names = "-minLength", description = "min average length of values in a cluster to be a candidate for pattern generation")
    private double minLength = 4;

    @Parameter(names = "-out")
    private String resultLocation;

    private static Set<String> stopwords = new HashSet<>(Arrays.asList(new String[] { "but", "be", "with", "such",
            "then", "for", "no", "will", "and", "their", "if", "this", "on", "into", "a", "or", "there", "in", "that",
            "they", "was", "is", "it", "an", "the", "as", "at", "these", "by", "to", "of", "i", "you" }));

    public static void main(String[] args) throws IOException {
        PatternCleaner app = new PatternCleaner();

        if (app.parseCommandLine(PatternCleaner.class, args)) {

            if (app.tableLocation == null) {
                demo();
            } else {
                app.run();
            }
        }
    }

    public static void demo() throws IOException {
        // String[] values = { "washington and lee at. univ. of the south | box score | 9/11/2010",
        //         "kean university at. the college of nj | box score | 10/20/2012",
        //         "st. lawrence at. hobart | box score | 10/1/2011" };
        // String[] correct = { "washington and lee univ. of the south 9/11/2010",
        //         "kean university the college of nj 10/20/2012", "st. lawrence hobart 10/1/2011" };
        String[] values = {
            "itunes - music - always  by natalia barbin & david sense",
            "itunes - music - universal masters collection: classic the platters by the platters",
            "itunes - music - more great dirt: the best of the nitty gritty dirt band, vol. 2 by nitty gritty dirt band",
            "itunes - música - the look of love de diana krall"
        };
        String[] result = new String[values.length];

        PatternCleaner pc = new PatternCleaner();
        // String pattern = pc.detectPattern(values);
        Set<String> valueSet = new HashSet<>(Arrays.asList(values));
        Set<String> patterns = pc.detectPatternsInClusters(valueSet, pc.createTokenIndex(valueSet), null, "");
        // System.out.println(pattern);

        Set<Pattern> compiled = new HashSet<>();
        for(String p : patterns) {
            compiled.add(Pattern.compile(p));
        }
        for (int i = 0; i < values.length; i++) {
            for(Pattern p : compiled) {
                Matcher m = p.matcher(values[i]);
                if (m.matches()) {
                    int gc = m.groupCount();
                    for (int j = 0; j < m.groupCount(); j++) {
                        if (j == 0) {
                            result[i] = m.group(j + 1);
                        } else {
                            result[i] += " " + m.group(j + 1);
                        }
                    }
                } else {
                    result[i] = values[i];
                }

                System.out.println(result[i]);
            }
        }
    }

    public void run() throws IOException {
        WebTables web = WebTables.loadWebTables(new File(tableLocation), true, true, false, true);

        BufferedWriter w = new BufferedWriter(new FileWriter(new File(resultLocation)));
        // if(resultLocation!=null) {
        //     w = new BufferedWriter(new FileWriter(new File(resultLocation)));
        // }

        try {
            new ParallelProcessableCollection<>(web.getTables().values()).foreach((Table t) -> {
            // for (Table t : web.getTables().values()) {
                System.out.println(t.getPath());
                Set<TableColumn> key = Q.firstOrDefault(t.getSchema().getCandidateKeys());
                // for(TableColumn c : t.getColumns()) {
                for (TableColumn c : key) {
                    if (c.getDataType() == DataType.string && !"fk".equalsIgnoreCase(c.getHeader())) {
                        System.out.println(c.getIdentifier() + " " + c.getHeader());
                        // Map<String, Set<String>> tokenIndex = new HashMap<>();
                        Set<String> values = new HashSet<>();

                        for(TableRow r : t.getRows()) {
                            String value = (String)r.get(c.getColumnIndex());

                            if(value!=null) {
                                values.add(value);

                                // for(String v : value.split("\\s")) {
                                //     if(!stopwords.contains(v)) {
                                //         MapUtils.getFast(tokenIndex, v, (k)->new HashSet<String>()).add(value);
                                //     }
                                // }
                            }
                        }
                        int uniqueValues = values.size();
                        System.out.println(String.format("%d unique values in %d rows", uniqueValues, t.getSize()));

                        Map<String, Set<String>> tokenIndex = createTokenIndex(values);
                        try {
                            detectPatternsInClusters(values, tokenIndex, w, c.getIdentifier());
                        } catch(Exception e) { e.printStackTrace(); }
                    }
                }
            // }
            });
        } finally {
            if(w!=null) {
                w.close();
            }
        }
    }

    public Map<String, Set<String>> createTokenIndex(Set<String> values) {
        Map<String, Set<String>> tokenIndex = new HashMap<>();
        for(String value : values) {
            for(String v : value.split("\\s")) {
                if(!stopwords.contains(v)) {
                    MapUtils.getFast(tokenIndex, v, (k)->new HashSet<String>()).add(value);
                }
            }
        }
        return tokenIndex;
    }

    public Set<String> detectPatternsInClusters(Set<String> values, Map<String, Set<String>> tokenIndex, BufferedWriter w, String setId) throws IOException {
        // create clusters of values from the most frequent tokens
        List<HashMap.Entry<String, Set<String>>> frequentTokens = MapUtils.sort(tokenIndex, (e1,e2)-> -Integer.compare(e1.getValue().size(), e2.getValue().size()));

        Set<String> cluster = null;
        int uniqueValues = values.size();
        Set<String> patterns = new HashSet<>();

        while(values.size()>0 && frequentTokens.size()>0) {
            // add all values containing the most frequent token to the same cluster
            HashMap.Entry<String, Set<String>> mostFrequent = frequentTokens.remove(0);
            // only use tokens which have not been considered before!
            cluster = Q.intersection(mostFrequent.getValue(), values);
            // and remove them from the value list
            values.removeAll(cluster);

            double avgTokens = Q.average(Q.project(cluster, (s)->s.split("\\s").length));
            double support = cluster.size()/(double)uniqueValues;

            if(support < minSupport) {
                // once we fall below min support, we can stop checking
                break;
            }

            if(avgTokens >= minLength) {
                System.out.println(String.format("\t%d values in cluster '%s' (support: %f) with an average of %f tokens per value", cluster.size(), mostFrequent.getKey(), support, avgTokens));

                // then detect the pattern for this cluster
                String pattern = detectPattern(cluster.toArray(new String[0]));
                patterns.add(pattern);

                System.out.println("\t" + pattern);

                if(w!=null) {
                    synchronized(w) {
                        w.write(String.format("%s\n", StringUtils.join(new String[] {
                            setId,
                            pattern,
                            Integer.toString(cluster.size()),
                            Integer.toString(uniqueValues),
                            Double.toString(support),
                            Double.toString(avgTokens)
                        }, "\t")));
                        w.flush();
                    }
                }

                // and continue with the next most frequent token, until all values have been used
            }
        }

        return patterns;
    }

    protected Map<String, Set<String>> patterns;

    public Map<String, Set<String>> loadPatterns(File f) throws IOException {
        BufferedReader r = new BufferedReader(new FileReader(f));
        String line = null;
        patterns = new HashMap<>();

        while((line = r.readLine())!=null) {
            String[] values = line.split("\t");
            if(values.length>1) {
                MapUtils.getFast(patterns, values[0], (p)->new HashSet<String>()).add(values[1]);
            }
        }

        r.close();

        return patterns;
    }

    public void cleanTable(Table t) {
        cleanTable(t, this.patterns);
    }

    public void cleanTable(Table t, Map<String, Set<String>> patterns) {
        for(TableColumn c : t.getColumns()) {
            if(c.getDataType()==DataType.string) {
                Set<String> pat = patterns.get(c.getIdentifier());
                Collection<Pattern> compiled = Q.project(pat, (s)->Pattern.compile(s));
                if(pat!=null) {
                    for(TableRow r : t.getRows()) {
                        Object obj = r.get(c.getColumnIndex());
                        if(obj!=null) {
                            if(!obj.getClass().isArray()) {
                                String value = (String)obj;
                                if(value!=null) {
                                    for(Pattern p : compiled) {
                                        Matcher m = p.matcher(value);
                                        if(m.matches()) {
                                            Collection<String> parts = new LinkedList<String>();
                                            for(int i = 0; i < m.groupCount(); i++) {
                                                parts.add(m.group(i+1));
                                            }
                                            r.set(c.getColumnIndex(), parts.toArray(new String[parts.size()]));
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public String detectPattern(String[] values) {
        String pattern = generatePattern(values);

        if(pattern!=null && !"".equals(pattern)) {
            return "(.*)" + pattern + "(.*)";
        } else {
            return "(.*)";
        }
    }

    protected String generatePattern(String[] values) {
        LCSubstringSolver solver = new LCSubstringSolver(new DefaultCharSequenceNodeFactory());
        int docs = 0;
        String pat = "";
        for(String v : values) {
            if(v!=null && !"".equals(v)) {
                solver.add(v);
                docs++;
            }
        }

        if(docs==values.length) {
            CharSequence sub = solver.getLongestCommonSubstring();

            if(!"".equals(sub) && sub.length()>1) {
                String[] left = new String[values.length];
                String[] right = new String[values.length];
                for(int i = 0; i < values.length; i++) {
                    String[] split = values[i].split(Pattern.quote(sub.toString()));
                    if(split.length>0) {
                        left[i] = split[0];
                    } else {
                        left[i] = "";
                    }
                    if(split.length>1) {
                        right[i] = split[1];
                    } else {
                        right[i] = "";
                    }
                }

                String leftP = generatePattern(left);
                String rightP = generatePattern(right);

                pat = Pattern.quote(sub.toString());
                System.out.println("\t\tpattern:" + pat);
                if(!"".equals(leftP)) {
                    pat = leftP + "(.*)" + pat;
                    System.out.println("\t\tleft:" + leftP);
                }
                if(!"".equals(rightP)) {
                    pat = pat + "(.*)" + rightP;
                    System.out.println("\t\tright:" + rightP);
                }
            }
        }

        return pat;
    }

}