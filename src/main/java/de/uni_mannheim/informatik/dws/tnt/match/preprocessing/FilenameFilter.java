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

import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DateJavaTime;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
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
public class FilenameFilter extends Executable {

    public static final Pattern filenamePattern = Pattern.compile("[\\w,-]+\\.(html|htm|asp|php)$");

    public static void main(String[] args) {
        String[] values = new String[] { "asd.html", "asfjjk asf asfd.htm", "test.asp", "asfasf.php", "asdasf.doc", "a v.", "asf asf .saf jkasf jasf sf" };
        FilenameFilter nf = new FilenameFilter();
        for(String v : values) {
            String cleaned = nf.removeFilenames(v);
            System.out.println(String.format("%s --> %s",v, cleaned));
        }
    }

    public FilenameFilter() {
    }

    public String removeFilenames(String text) {
        Matcher m = filenamePattern.matcher(text);
        // if(m.matches()) {
            text = m.replaceAll("");
            if(text.trim().length()==0) {
                text=null;
            }
        // }
        return text;
    }

    public void cleanTable(Table t) {
        for(TableColumn c : t.getColumns()) {
            if(c.getDataType()==DataType.string) {
                for(TableRow r : t.getRows()) {
                    Object obj = r.get(c.getColumnIndex());
                    if(obj!=null) {
                        if(!obj.getClass().isArray()) {
                            String value = (String)obj;
                            Matcher m = filenamePattern.matcher(value);
                            value = m.replaceAll("");
                            if(value.trim().length()==0) {
                                value=null;
                            }
                            r.set(c.getColumnIndex(), value);
                        } else {
                            Object[] list = (Object[]) obj;
                            for(int i = 0; i < list.length; i++) {
                                Object o = list[i];
                                if(o!=null) {
                                    String value = (String)o;
                                    Matcher m = filenamePattern.matcher(value);
                                    value = m.replaceAll("");
                                    if(value.trim().length()==0) {
                                        value=null;
                                    }
                                    r.set(c.getColumnIndex(), value);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}