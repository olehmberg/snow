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
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import org.apache.commons.lang.StringUtils;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.deser.DataFormatReaders.Match;

import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.matchers.MatchingRuleGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.MatchableTableRowToKBComparator;
import de.uni_mannheim.informatik.dws.tnt.match.similarity.TFIDF;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.RuleLearner;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer.VectorCreationMethod;
import de.uni_mannheim.informatik.dws.winter.matching.rules.MatchingRule;
import de.uni_mannheim.informatik.dws.winter.matching.rules.WekaMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence.RecordId;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.FeatureVectorDataSet;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.processing.*;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class GenerateRecordLinkageFeatures extends Executable {

    @Parameter(names = "-cor")
    private String correspondenceLocation;

    @Parameter(names = "-gs")
    private String gsLocation;

    @Parameter(names = "-web")
    private String webLocation;

    @Parameter(names = "-kb")
    private String kbLocation;

    @Parameter(names = "-out")
    private String out;

    public static void main(String[] args) throws IOException {
        GenerateRecordLinkageFeatures app = new GenerateRecordLinkageFeatures();

        if(app.parseCommandLine(GenerateRecordLinkageFeatures.class, args)) {
            app.run();
        }
    }

    public void run() throws IOException {

        WebTables web = WebTables.loadWebTables(new File(webLocation), true, true, true, false);
        KnowledgeBase kb = KnowledgeBase.loadKnowledgeBase(new File(kbLocation), null, null, true, true);

        Processable<Correspondence<RecordId, RecordId>> correspondences = Correspondence.loadFromCsv(new File(correspondenceLocation));

        // get best candidate for each correspondence
        Processable<Correspondence<RecordId, RecordId>> bestCandidates = correspondences
            .group(
                (Correspondence<RecordId, RecordId> c, DataIterator<Pair<RecordId,Correspondence<RecordId, RecordId>>> col)
                -> col.next(new Pair<RecordId, Correspondence<RecordId,RecordId>>(c.getFirstRecord(), c))
            )
            .map(
                (Group<RecordId, Correspondence<RecordId,RecordId>> g)
                ->Q.max(g.getRecords().get(), (c)->c.getSimilarityScore())
            );

        // get the correct candidates for each example
        MatchingGoldStandard gs = new MatchingGoldStandard();
        gs.loadFromTSVFile(new File(gsLocation));

        Processable<Correspondence<RecordId, RecordId>> correctCandidates = correspondences
            .join(new ParallelProcessableCollection<>(gs.getPositiveExamples()),
                (c)->new Pair<>(c.getFirstRecord().getIdentifier(), c.getSecondRecord().getIdentifier()),
                (p)->p
            )
            .map((p)->p.getFirst());

        Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = createSchemaCorrespondences(web,kb);
        WekaMatchingRule<MatchableTableRow, MatchableTableColumn> rule = createMatchingRule(web, kb, schemaCorrespondences);

        FeatureVectorDataSet features = rule.initialiseFeatures(web.getRecords().getRandomRecord(), kb.getRecords().getRandomRecord(), schemaCorrespondences);
        for(Attribute a : features.getSchema().get()) {
            System.out.println(String.format("Feature: %s", a.getIdentifier()));
        }

        List<String> headers = new LinkedList<>();
        boolean createHeaders = true;

        BufferedWriter w = new BufferedWriter(new FileWriter(new File(out)));

        for(Correspondence<RecordId, RecordId> candidate : bestCandidates.append(correctCandidates).get()) {
            MatchableTableRow webRecord = web.getRecords().getRecord(candidate.getFirstRecord().getIdentifier());
            MatchableTableRow kbRecord = kb.getRecords().getRecord(candidate.getSecondRecord().getIdentifier());
            Record r = rule.generateFeatures(webRecord, kbRecord, schemaCorrespondences, features);
            if(gs.containsPositive(webRecord, kbRecord)) {
                r.setValue(FeatureVectorDataSet.ATTRIBUTE_LABEL, "1");
            } else {
                r.setValue(FeatureVectorDataSet.ATTRIBUTE_LABEL, "0");
            }
            
            List<String> values = new LinkedList<>();
            for(MatchableTableColumn c : webRecord.getSchema()) {
                if(!"pk".equals(c.getHeader())) {
                    Object value = webRecord.get(c.getColumnIndex());
                    if(value==null) {
                        values.add("null");
                    } else {
                        values.add(value.toString());
                    }
                    if(createHeaders) {
                        headers.add(c.getHeader());
                    }
                }
            }

            for(MatchableTableColumn c : kbRecord.getSchema()) {
                if(!"pk".equals(c.getHeader())) {
                    Object value = kbRecord.get(c.getColumnIndex());
                    if(value==null) {
                        values.add("null");
                    } else {
                        values.add(value.toString());
                    }
                    if(createHeaders) {
                        headers.add(c.getHeader());
                    }
                }
            }

            for(Attribute a : features.getSchema().get()) {
                if("label".equals(a.getIdentifier()) || a.getIdentifier()!=null && a.getIdentifier().startsWith("[")) {
                    values.add(r.getValue(a));
                    if(createHeaders) {
                        headers.add(a.getIdentifier());
                    }
                }
            }

            if(createHeaders) {
                w.write(String.format("%s\n", StringUtils.join(headers, "\t")));
                createHeaders = false;
            }
            w.write(String.format("%s\n", StringUtils.join(values, "\t")));
        }

        // for(Correspondence<RecordId, RecordId> candidate : bestCandidates.get()) {
        //     MatchableTableRow webRecord = web.getRecords().getRecord(candidate.getFirstRecord().getIdentifier());
        //     MatchableTableRow kbRecord = kb.getRecords().getRecord(candidate.getSecondRecord().getIdentifier());
        //     Record r = rule.generateFeatures(webRecord, kbRecord, schemaCorrespondences, features);
        //     r.setValue(FeatureVectorDataSet.ATTRIBUTE_LABEL, "0");
            
        //     List<String> values = new LinkedList<>();
        //     for(MatchableTableColumn c : webRecord.getSchema()) {
        //         if(!"pk".equals(c.getHeader())) {
        //             Object value = webRecord.get(c.getColumnIndex());
        //             if(value==null) {
        //                 values.add("null");
        //             } else {
        //                 values.add(value.toString());
        //             }
        //         }
        //     }

        //     for(MatchableTableColumn c : kbRecord.getSchema()) {
        //         if(!"pk".equals(c.getHeader())) {
        //             Object value = kbRecord.get(c.getColumnIndex());
        //             if(value==null) {
        //                 values.add("null");
        //             } else {
        //                 values.add(value.toString());
        //             }
        //         }
        //     }

        //     for(Attribute a : features.getSchema().get()) {
        //         if("label".equals(a.getIdentifier()) || a.getIdentifier()!=null && a.getIdentifier().startsWith("[")) {
        //             values.add(r.getValue(a));
        //         }
        //     }

        //     w.write(String.format("%s\n", StringUtils.join(values, "\t")));
        // }

        w.close();
    }

    private Processable<Correspondence<MatchableTableColumn, Matchable>> createSchemaCorrespondences(WebTables web, KnowledgeBase kb) {
        Table t = Q.firstOrDefault(web.getTables().values());
        Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = new ParallelProcessableCollection<>();
        for(TableColumn c : t.getColumns()) {
            if(t.getMapping().getMappedProperty(c.getColumnIndex())!=null) {
                MatchableTableColumn webCol = web.getSchema().getRecord(c.getIdentifier());
                MatchableTableColumn kbCol = kb.getSchema().getRecord(t.getMapping().getMappedProperty(c.getColumnIndex()).getFirst());
                schemaCorrespondences.add(new Correspondence<>(webCol, kbCol, 1.0));
                System.out.println(String.format("%s -> %s",
                    webCol.getHeader(),
                    kbCol.getHeader()
                ));
            } else {
                System.out.println(String.format("%s not mapped!", c.getHeader()));
            }
        }
        return schemaCorrespondences;
    }

    private WekaMatchingRule<MatchableTableRow, MatchableTableColumn> createMatchingRule(WebTables web, KnowledgeBase kb, Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {

        Table t = Q.firstOrDefault(web.getTables().values());
        int classId = kb.getClassIds().get(t.getMapping().getMappedClass().getFirst());
        String className = t.getMapping().getMappedClass().getFirst();

        DataSet<MatchableTableRow, MatchableTableColumn> kbRecords = new ParallelHashedDataSet<>(kb.getRecords().where((r)->r.getTableId()==classId).get());
        Processable<MatchableTableColumn> targetSchema = kb.getSchema().where((c)->c.getTableId()==classId);

        MatchableTableColumn labelColumn = targetSchema.where((c)->KnowledgeBase.RDFS_LABEL.equals(c.getHeader())).firstOrNull();

		TFIDF<String> weights = new TFIDF<>(
			(s)->s.split(" "),
			VectorCreationMethod.TFIDF);
		
		weights.prepare(kbRecords, (record,col)->{
			Object label = record.get(labelColumn.getColumnIndex());
			Set<String> values = new HashSet<>();
			if(label instanceof String[]) {
				values.addAll(Arrays.asList((String[])label));
			} else {
				values.add((String)label);
			}

			for(String value : values) {
				if(value!=null) {
					col.next(MatchableTableRowToKBComparator.preprocessKBString(value));
				}
			}
		});

        System.out.println(String.format("[EntityTableMatcher.learnMatchingRule] Generating matching rule for target class '%s' with schema '%s'", 
            className,
            Q.project(targetSchema.sort((c)->c.getColumnIndex()).get(), (c)->c.getHeader())
        ));
		
		MatchingRuleGenerator ruleGenerator = new MatchingRuleGenerator(kb, 0.5);
		ruleGenerator.setWeights(weights);
		WekaMatchingRule<MatchableTableRow, MatchableTableColumn> wekaRule = ruleGenerator.createWekaMatchingRule(targetSchema, schemaCorrespondences, "SimpleLogistic", new String[] {});

        return wekaRule;
    }

}