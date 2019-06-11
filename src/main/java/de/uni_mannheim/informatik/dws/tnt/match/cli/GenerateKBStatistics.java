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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import com.beust.jcommander.Parameter;
import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.processing.*;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SumDoubleAggregator;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class GenerateKBStatistics extends Executable {

    @Parameter(names = "-hierarchy")
    private String hierarchyLocation;

    public static void main(String[] args) throws IOException {
        GenerateKBStatistics app = new GenerateKBStatistics();

        if(app.parseCommandLine(GenerateKBStatistics.class, args)) {
            app.run();
        }
    }

    public void run() throws IOException {
        if(params.size()>0) {

            if(hierarchyLocation!=null) {
                KnowledgeBase.loadClassHierarchy(hierarchyLocation);
            }
            KnowledgeBase kb = KnowledgeBase.loadKnowledgeBase(new File(params.get(0)), null, null, false, false);

            Processable<Pair<Integer, Double>> recordsPerClass = kb.getRecords().aggregate(new RecordKeyValueMapper<Integer,MatchableTableRow,Double>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void mapRecordToKey(MatchableTableRow record,
						DataIterator<Pair<Integer, Double>> resultCollector) {
                        resultCollector.next(new Pair<>(record.getTableId(), 1.0));
				}
            }, new SumDoubleAggregator<>());

            Map<Integer, Double> recordsByClass = Pair.toMap(recordsPerClass.get());

            for(int cls : kb.getClassIndices().keySet()) {
                String clsName = kb.getClassIndices().get(cls);
                int rows = 0;
                if(recordsByClass.containsKey(cls)) {
                    rows = recordsByClass.get(cls).intValue();
                }

                System.out.println(StringUtils.join(new String[] {
                    clsName,
                    Integer.toString(rows)
                }, "\t"));
            }

        }
    }

}