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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.time.*;
import edu.stanford.nlp.util.CoreMap;

import com.beust.jcommander.Parameter;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharSequenceNodeFactory;
import com.googlecode.concurrenttrees.solver.LCSubstringSolver;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DateJavaTime;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.utils.parallel.Parallel;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * Detects and removes date and time values from strings
 * 
  * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TimeExtractor extends Executable {

    @Parameter(names = "-web") 
    private String webLocation;

    @Parameter(names = "-out") 
    private String resultLocation;

    public static void main(String[] args) throws Exception {
        TimeExtractor app = new TimeExtractor();

        if (app.parseCommandLine(TimeExtractor.class, args)) {

            if(app.webLocation!=null) {
                app.run();
            } else {
                app.demo();
            }
        }
    }

    public void demo() {
        String[] values = new String[] { 
            "september 1", "1998", "16", "13", "theater counts for week 49 of 2011", 
            "sun", "mon", "tue", "wed", "thu", "fri", "sat", "2 p.m. ct", "2:30 p.m. ct",
            "6th", "1975/76", "06", "1987", "top february opening weekends at the box office",
            "23 sec", "1.63 sec", "2 min 25 sec", "november 2325, 2007"
        };

        TimeExtractor te = new TimeExtractor();

        for(String s : values) {
            // List<Pair<Integer, Integer>> offsets = te.detectTime(s);
            // String cleaned = te.removeTime(s, offsets);
            String cleaned = te.removeTime(s);
            System.out.println(String.format("%s --> %s", s, cleaned));
        }
    }

    public void run() throws Exception {
        Parallel.setReportIfStuck(false);
        // load web tables
        File webFile = new File(webLocation);
        WebTables web = WebTables.loadWebTables(webFile, true, true, false, false);

        TimeExtractor te = new TimeExtractor();

        Collection<Table> relationTables = Q.where(web.getTables().values(), (t) -> t.getPath().contains("_rel_"));

        BufferedWriter w = new BufferedWriter(new FileWriter(new File(resultLocation)));
        String header = String.format("%s\n", StringUtils.join(new String[] {
            "Column Id",
            "Date Count",
            "Table Rows",
            "Table Columns",
            "Table Class",
            "Column Header",
            "Is Context",
            "Key Size"
        }, "\t"));
        w.write(header);
        
        Processable<Table> tables = new ParallelProcessableCollection<>(relationTables);
        tables.foreach((Table t)->{
        // for (Table t : relationTables) {
            Set<TableColumn> key = Q.firstOrDefault(t.getSchema().getCandidateKeys());

            // System.out.println(String.format("Table %s", t.getPath()));

            Iterator<TableColumn> it = key.iterator();
            while (it.hasNext()) {
                TableColumn c = it.next();
                if ("fk".equals(c.getHeader()) || c.getDataType() != DataType.string) {
                    it.remove();
                    // System.out.println(String.format("\tRemoving %s: '%s'", c.getIdentifier(), c.getHeader()));
                } else {
                    // System.out.println(String.format("\tKeeping %s: '%s'", c.getIdentifier(), c.getHeader()));
                }
            }

            try {
                Table projection = t.project(key);
                projection.setTableId(t.getTableId());
                
                // Map<TableColumn, Integer> timeOccurrences = te.cleanTable(t);
                Map<TableColumn, Integer> timeOccurrences = te.cleanTable(projection);

                String tblClass = null;
                Pair<String, Double> cls = t.getMapping().getMappedClass();
                if(cls!=null) {
                    tblClass = cls.getFirst();
                } else {
                    tblClass = t.getPath().split("\\_")[0];
                }

                for(TableColumn c : timeOccurrences.keySet()) {
                    Integer cnt = timeOccurrences.get(c);
                    // System.out.println(String.format("%s\t%d", c.getIdentifier(), cnt));
                    if(cnt!=null) {
                        // synchronized(w) {
                            w.write(String.format("%s\n", StringUtils.join(new String[] {
                                c.getIdentifier(),
                                Integer.toString(cnt),
                                Integer.toString(t.getSize()),
                                Integer.toString(t.getColumns().size()),
                                tblClass,
                                c.getHeader(),
                                Boolean.toString(ContextColumns.isContextColumn(c)),
                                Integer.toString(key.size())
                            }, "\t")));
                            w.flush();
                        // }
                    }
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        });
        w.close();
        
    }

    protected AnnotationPipeline pipeline;

    public TimeExtractor() {
        Properties props = new Properties();
        pipeline = new AnnotationPipeline();
        pipeline.addAnnotator(new WhitespaceTokenizerAnnotator(props));
        pipeline.addAnnotator(new WordsToSentencesAnnotator(false));
        pipeline.addAnnotator(new POSTaggerAnnotator(false));
        pipeline.addAnnotator(new TimeAnnotator("sutime", props));
    }

    public List<Pair<Integer, Integer>> detectTime(String text) {
        Annotation annotation = new Annotation(text);
        annotation.set(CoreAnnotations.DocDateAnnotation.class, "2013-07-14");
        pipeline.annotate(annotation);
        // System.out.println(annotation.get(CoreAnnotations.TextAnnotation.class));
        List<CoreMap> timexAnnsAll = annotation.get(TimeAnnotations.TimexAnnotations.class);
        List<Pair<Integer, Integer>> offsets = new LinkedList<>();
        for (CoreMap cm : timexAnnsAll) {
            List<CoreLabel> tokens = cm.get(CoreAnnotations.TokensAnnotation.class);
            int from = tokens.get(0).get(CoreAnnotations.CharacterOffsetBeginAnnotation.class);
            int to = tokens.get(tokens.size() - 1).get(CoreAnnotations.CharacterOffsetEndAnnotation.class);
            // System.out.println("\t" + cm + " [from char offset " +
            //     from +
            //     " to " + to + ']' +
            //     " --> " + cm.get(TimeExpression.Annotation.class).getTemporal());

            offsets.add(new Pair<>(from , to));
        }
        // System.out.println("--");
        return offsets;
    }

    public String removeTime(String text) {
        List<Pair<Integer, Integer>> offsets = detectTime(text);
        return removeTime(text, offsets);
    }

    public String removeTime(String text, List<Pair<Integer, Integer>> offsets) {
        int pos = 0;
        StringBuilder sb = new StringBuilder();
        for(Pair<Integer, Integer> p : offsets) {
            sb.append(text.substring(pos, p.getFirst()));
            pos = p.getSecond()+1;
        }
        if(pos<text.length()) {
            sb.append(text.substring(pos, text.length()));
        }
        if(sb.length()==0) {
            return null;
        } else {
            return sb.toString();
        }
    }

    public Map<TableColumn, Integer> cleanTable(Table t) {
        Map<TableColumn, Integer> timeOccurrences = new HashMap<>();
        Processable<TableRow> cols = new ParallelProcessableCollection<>(t.getRows());
        for(TableColumn c : t.getColumns()) {
            if(c.getDataType()==DataType.string) {
                for(TableRow r : t.getRows()) {
                    Object obj = r.get(c.getColumnIndex());
                    if(obj!=null) {
                        if(!obj.getClass().isArray()) {
                            String value = (String)obj;
                            if(value!=null) {
                                List<Pair<Integer, Integer>> offsets = detectTime(value);
                                if(offsets.size()>0) {
                                    MapUtils.increment(timeOccurrences, c);
                                    String cleaned = removeTime(value, offsets);
                                    r.set(c.getColumnIndex(), cleaned);
                                }
                            }
                        } else {
                            Object[] list = (Object[]) obj;
                            boolean hasTime = false;
                            for(int i = 0; i < list.length; i++) {
                                Object o = list[i];
                                if(o!=null) {
                                    String value = (String)o;
                                    List<Pair<Integer, Integer>> offsets = detectTime(value);
                                    if(offsets.size()>0) {
                                        String cleaned = removeTime(value);
                                        list[i] = cleaned;
                                        hasTime = true;
                                    }
                                }
                            }
                            if(hasTime) {
                                MapUtils.increment(timeOccurrences, c);
                            }
                        }
                    }
                }
            }
        }

        return timeOccurrences;
    }
}