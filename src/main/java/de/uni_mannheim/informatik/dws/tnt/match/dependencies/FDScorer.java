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
package de.uni_mannheim.informatik.dws.tnt.match.dependencies;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.StitchedModel;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.RecordCSVFormatter;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.Table.ConflictHandling;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class FDScorer {

    public static interface FDScoringFunction {
        double score(Pair<Set<TableColumn>,Set<TableColumn>> fd, Table t, StitchedModel model);
    }

    public static final FDScoringFunction lengthScore = (fd,t,model) -> 0.5 * (1.0/(double)fd.getFirst().size() + getNonContextAttributes(fd.getSecond()).size()/(double)longestRHS(t));

    public static Set<TableColumn> getNonContextAttributes(Set<TableColumn> attributes) {
        return new HashSet<>(Q.where(attributes, (c)->!ContextColumns.isRenamedUnionContextColumn(c.getHeader())));
    }

    public static int longestRHS(Table t) {
        return Math.max(1,Q.<Integer>max(Q.project(t.getSchema().getFunctionalDependencies().values(), (rhs)->getNonContextAttributes(rhs).size())));
    }

    public static final FDScoringFunction positionScore = (fd,t,model) -> 0.5 * (1.0/(double)(attributesBetween(fd.getFirst())+1) + 1/(double)(attributesBetween(fd.getSecond())+1));

    public static double attributesBetween(Set<TableColumn> attributes) {
        Collection<TableColumn> nonContext = Q.where(attributes, (c)->!ContextColumns.isContextColumn(c));
        if(nonContext.size()==0) {
            return 1.0;
        }
        List<Integer> positions = Q.<Integer>sort(Q.project(nonContext, (c)->c.getColumnIndex()));
        int between = 0;
        int last = -1;
        for(int i : positions) {
            if(last==-1) {
                last = i;
            } else {
                between += i - last - 1;
                last = i;
            }
        }
        return between;
    }

    public static final FDScoringFunction dataTypeScore = (fd,t,model) -> Q.where(fd.getFirst(), (c)->c.getDataType()==DataType.string || c.getDataType()==DataType.date).size()/(double)fd.getFirst().size();

    public static final FDScoringFunction determinantProvenanceScore = (fd,t,model) -> model.getProvenanceForColumnCombination(fd.getFirst()).size()/(double)model.getStitchedProvenance().get(t).size();
    public static final FDScoringFunction dependantProvenanceScore = (fd,t,model) -> model.getProvenanceForColumnCombination(fd.getSecond()).size()/(double)model.getStitchedProvenance().get(t).size();

    public static final FDScoringFunction weakFDScore = (fd,t,model) -> model.getProvenanceForColumnCombination(Q.union(fd.getFirst(),fd.getSecond())).size()/(double)model.getProvenanceForColumnCombination(fd.getFirst()).size();
    public static final FDScoringFunction strongFDScore = (fd,t,model) -> model.getProvenanceForColumnCombination(fd.getSecond()).containsAll(model.getProvenanceForColumnCombination(Q.union(fd.getFirst(),fd.getSecond()))) ? 1.0 : 0.0;

    public static final FDScoringFunction extensionalDeterminantProvenanceScore = (fd,t,model) -> 
        Q.<Integer>sum(Q.project(model.getProvenanceForColumnCombination(fd.getFirst()), (t0)->t0.getSize()))
        /
        (double)Q.<Integer>sum(Q.project(model.getStitchedProvenance().get(t), (t0)->t0.getSize()));

    public static final FDScoringFunction extensionalDependantProvenanceScore = (fd,t,model) -> 
        Q.<Integer>sum(Q.project(model.getProvenanceForColumnCombination(fd.getSecond()), (t0)->t0.getSize()))
        /
        (double)Q.<Integer>sum(Q.project(model.getStitchedProvenance().get(t), (t0)->t0.getSize()));

    public static final FDScoringFunction duplicationScore = (fd,t,model) -> 0.5 * (2.0 - getUniquenessScore(fd.getFirst(), t) - getUniquenessScore(fd.getSecond(), t));

    public static double getUniquenessScore(Collection<TableColumn> attributes, Table t) {
        try {
            Table reducedTable = t.project(attributes);
            int emptyRows = 0;
            for(TableRow r : reducedTable.getRows()) {
                boolean hasValues = false;
                for(TableColumn c : reducedTable.getColumns()) {
                    if(r.get(c.getColumnIndex())!=null) {
                        hasValues=true;
                        break;
                    }
                }
                if(!hasValues) {
                    emptyRows++;
                }
            }
            int totalValues = reducedTable.getSize() - emptyRows;
            reducedTable.deduplicate(reducedTable.getColumns(), ConflictHandling.KeepFirst, false);
            int uniqueValues = reducedTable.getSize();

            return uniqueValues / (double)totalValues;
        } catch(Exception e) {
            return 0;
        }
    }

    public static final FDScoringFunction conflictRateScore = (fd,t,model) -> 1.0 - getConflictRate(fd.getFirst(), t);

    public static double getConflictRate(Collection<TableColumn> attributes, Table t) {
        try {
            Table reducedTable = t.project(attributes);
            ArrayList<TableRow> nonEmptyRows = new ArrayList<>(reducedTable.getSize());
            for(TableRow r : reducedTable.getRows()) {
                boolean hasValues = false;
                for(TableColumn c : reducedTable.getColumns()) {
                    if(r.get(c.getColumnIndex())!=null) {
                        hasValues=true;
                        break;
                    }
                }
                if(hasValues) {
                    nonEmptyRows.add(r);
                }
            }
            nonEmptyRows.trimToSize();
            reducedTable.setRows(nonEmptyRows);
            int totalValues = reducedTable.getSize();
            Set<TableRow> conflictingRows = reducedTable.findUniquenessViolations(reducedTable.getColumns());

            return conflictingRows.size() / (double)totalValues;
        } catch(Exception e) {
            return 0;
        }
    }

    /**
     * Measures in how many of all determinants the FD's determinant attributes are contained (individually)
     */
    public static final FDScoringFunction attributeDeterminantFrequency = (fd,t,model) -> {
        int freq = 0;
        for(TableColumn c : fd.getFirst()) {
            freq += Q.where(t.getSchema().getFunctionalDependencies().keySet(), (det)->det.contains(c)).size();
        }
        return freq / (double)(fd.getFirst().size() * t.getSchema().getFunctionalDependencies().size());
    };

    /**
     * Measures in how many of all determinants the FD's determinant attributes are contained (combined)
     */
    public static final FDScoringFunction attributeCombinationDeterminantFrequency = (fd,t,model) -> {
        int freq= Q.where(t.getSchema().getFunctionalDependencies().keySet(), (det)->det.containsAll(fd.getFirst())).size();
        return freq / (double)t.getSchema().getFunctionalDependencies().size();
    };

    /**
     * Measures in how many of all dependants the FD's determinant attributes are contained (individually)
     */
    public static final FDScoringFunction attributeDependantFrequency = (fd,t,model) -> {
        int freq = 0;
        for(TableColumn c : fd.getFirst()) {
            freq += Q.where(t.getSchema().getFunctionalDependencies().values(), (det)->det.contains(c)).size();
        }
        return freq / (double)(fd.getFirst().size() * t.getSchema().getFunctionalDependencies().size());
    };

    public static final FDScoringFunction disambiguation = (fd,t,model) -> {
        for(TableColumn c : fd.getFirst()) {
            if(ContextColumns.isDisambiguationColumn(c.getHeader())) {
                return 1.0;
            }
        }

        return 0.0;
    };

    public static final FDScoringFunction disambiguationOfForeignKey = (fd,t,model) -> {
        Set<String> attributeNames = new HashSet<>(Q.project(t.getColumns(), (c)->c.getHeader().trim()));

        for(TableColumn c : fd.getFirst()) {
            if(ContextColumns.isDisambiguationColumn(c.getHeader()) && !attributeNames.contains(ContextColumns.getDisambiguatedColumn(c.getHeader()).trim())) {
                // System.out.println(String.format("FK disambiguation: %s / %s", c.getHeader(), ContextColumns.getDisambiguatedColumn(c.getHeader()).trim()));
                return 1.0;
            }
        }

        return 0.0;
    };

    public static final FDScoringFunction filenameScore = (fd,t,model) -> containsFileName(fd.getFirst(), t) ? 0.0 : 1.0;

    public static final Pattern filenamePattern = Pattern.compile("^[\\w,-]+\\.\\w+$");
    public static boolean containsFileName(Set<TableColumn> attributes, Table t) {
        Map<TableColumn,Integer> seenValues = new HashMap<>();
        for(TableRow r : t.getRows()) {
            for(TableColumn attribute : attributes) {
                // only test string attributes
                // only check the first 10 non-null values (if an attribute contains filenames, all values should be filenames)
                if(attribute.getDataType()==DataType.string && MapUtils.get(seenValues,attribute,0)<10) {
                    Object objValue = r.get(attribute.getColumnIndex());
                    if(objValue!=null) {
                        MapUtils.increment(seenValues, attribute);
                        if(filenamePattern.matcher(objValue.toString()).matches()) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    public static final FDScoringFunction disambiguationSplitScore = (fd,t,model) -> containsIncompleteDisambiguation(new HashSet<>(Q.union(fd.getFirst(),fd.getSecond())), t) ? 0.0 : 1.0;

    public static boolean containsIncompleteDisambiguation(Set<TableColumn> attributes, Table t) {
        Set<String> attributeNames = new HashSet<>(Q.project(attributes, (c)->c.getHeader().trim()));
        Set<String> allAttributeNames = new HashSet<>(Q.project(t.getColumns(), (c)->c.getHeader().trim()));

        for(TableColumn c : attributes) {
            if(
                // there is a disambiguation column
                ContextColumns.isDisambiguationColumn(c.getHeader()) 
                && 
                // which does not disambiguate any of the other attributes
                !attributeNames.contains(ContextColumns.getDisambiguatedColumn(c.getHeader()).trim())
                &&
                // and it is not because the disambiguated attribute is part of the FK
                allAttributeNames.contains(ContextColumns.getDisambiguatedColumn(c.getHeader()).trim())
            ) {
                return true;
            }
        }

        return false;
    }

    private List<Pair<FDScoringFunction,Attribute>> scoringFunctions;
    private DataSet<Record,Attribute> features;
    private Attribute determinant = new Attribute("determinant");
    private Attribute dependant = new Attribute("dependant");
    private Attribute finalScore = new Attribute("final_score");

    /**
     * @return the features
     */
    public DataSet<Record,Attribute> getFeatures() {
        return features;
    }

    public FDScorer() {
        scoringFunctions = new LinkedList<>();
        features = new HashedDataSet<>();
        features.addAttribute(determinant);
        features.addAttribute(dependant);
        features.addAttribute(finalScore);
    }

    public void addScoringFunction(FDScoringFunction f, String name) {
        Attribute att = new Attribute(name);
        scoringFunctions.add(new Pair<>(f,att));
        features.addAttribute(att);
    }

    public List<Pair<Pair<Set<TableColumn>,Set<TableColumn>>,Double>> scoreFunctionalDependenciesForDecomposition(Collection<Pair<Set<TableColumn>,Set<TableColumn>>> functionalDependencies, Table t, StitchedModel model) {

        features.ClearRecords();

        List<Pair<Pair<Set<TableColumn>,Set<TableColumn>>,Double>> result = new ArrayList<>(functionalDependencies.size());

        for(Pair<Set<TableColumn>,Set<TableColumn>> fd : functionalDependencies) {
            double score = 0.0;

            String identifier = String.format("%d:%s",t.getTableId(),StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","));
            String det = StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ",");
            String dep = StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ",");

            Record featureValues = new Record(identifier);
            featureValues.setValue(determinant, det);
            featureValues.setValue(dependant, dep);

            for(Pair<FDScoringFunction,Attribute> p : scoringFunctions) {
                FDScoringFunction f = p.getFirst();
                Attribute att = p.getSecond();

                double s = f.score(fd, t, model);
                score += s;

                featureValues.setValue(att, Double.toString(s));
            }

            score = score / (double)scoringFunctions.size();

            featureValues.setValue(finalScore, Double.toString(score));

            result.add(new Pair<>(fd, score));
            features.add(featureValues);
        }

        Collections.sort(result, (p1,p2)->-Double.compare(p1.getSecond(), p2.getSecond()));

        return result;
    }

    public String formatScores() {
        RecordCSVFormatter formatter = new RecordCSVFormatter();
        StringBuilder sb = new StringBuilder();
        List<Attribute> headers = new ArrayList<>(features.getSchema().get());
        sb.append(StringUtils.join(formatter.getHeader(headers), "\t"));
        sb.append("\n");
        for(Record record : features.get()) {
            sb.append(StringUtils.join(formatter.format(record, getFeatures(), headers), "\t"));
            sb.append("\n");
        }
        return sb.toString();
    }

    public double calculateScore(Pair<Set<TableColumn>,Set<TableColumn>> fd, Table t, StitchedModel model) {
        double score = 0.0;

        for(Pair<FDScoringFunction,Attribute> p : scoringFunctions) {
            FDScoringFunction f = p.getFirst();

            double s = f.score(fd, t, model);
            score += s;

        }

        score = score / (double)scoringFunctions.size();

        return score;
    }

    public Comparator<Pair<Collection<TableColumn>,Collection<TableColumn>>> createFDComparator(Table t, StitchedModel model) {
        return new Comparator<Pair<Collection<TableColumn>,Collection<TableColumn>>>() {

			@Override
			public int compare(Pair<Collection<TableColumn>, Collection<TableColumn>> o1,
					Pair<Collection<TableColumn>, Collection<TableColumn>> o2) {
                double score1 = calculateScore(new Pair<>(new HashSet<>(o1.getFirst()), new HashSet<>(o2.getSecond())), t, model);
                double score2 = calculateScore(new Pair<>(new HashSet<>(o2.getFirst()), new HashSet<>(o2.getSecond())), t, model);
                return -Double.compare(score1,score2);
			}

        };
    }

    

}