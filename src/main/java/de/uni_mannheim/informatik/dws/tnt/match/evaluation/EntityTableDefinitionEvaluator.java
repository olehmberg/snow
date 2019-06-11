package de.uni_mannheim.informatik.dws.tnt.match.evaluation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import de.uni_mannheim.informatik.dws.snow.SnowPerformance;
import de.uni_mannheim.informatik.dws.tnt.match.data.EntityTable;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.StitchedModel;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FDScorer;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.StitchedFunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.MaximumBipartiteMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.model.BigPerformance;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence.RecordId;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.Table.ConflictHandling;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import edu.stanford.nlp.util.StringUtils;

public class EntityTableDefinitionEvaluator {

	public Map<EntityTable, EntityTable> mapReferenceToDetectedEntityTables(
		Collection<EntityTable> entityTables,
		Collection<EntityTable> referenceEntityTables,
		Map<String, Integer> originalWebTableCount
	) {
		Map<String, EntityTable> entityIds = new HashMap<>();

		EntityTable detectedNE = null;
		EntityTable referenceNE = null;

		// assign IDs to the entity tables
		for(EntityTable ent : entityTables) {
			if(ent.isNonEntity()) {
				detectedNE = ent;
			} else {
				entityIds.put(ent.getEntityName(), ent);
			}
		}
		for(EntityTable ent : referenceEntityTables) {
			if(ent.isNonEntity()) {
				referenceNE = ent;
			} else {
				entityIds.put("reference::" + ent.getEntityName(), ent);
			}
		}

		// create correspondences between  entity tables
		// - the similarity score is the number of matching input tables
		Processable<Correspondence<RecordId, RecordId>> correspondences = new ParallelProcessableCollection<>();
		for(EntityTable ent : entityTables) {
			if(!ent.isNonEntity()) {
				for(EntityTable ref : entityTables) {
					if(!ref.isNonEntity()) {
						Set<String> entProvenance = ent.getProvenance();
						Set<String> refProvenance = ref.getProvenance();

						int intersectionSize = getOriginalColumnCount(Q.intersection(entProvenance, refProvenance), originalWebTableCount);

						// Correspondence<RecordId, RecordId> cor = new Correspondence<>(new RecordId(ent.getEntityName()), new RecordId("reference::" + ref.getEntityName()), Q.intersection(entProvenance, refProvenance).size());
						Correspondence<RecordId, RecordId> cor = new Correspondence<>(new RecordId(ent.getEntityName()), new RecordId("reference::" + ref.getEntityName()), intersectionSize);
						// System.out.println(String.format("Provenance overlap between %s and %s: %d tables", ent.getEntityName(), ref.getEntityName(), (int)cor.getSimilarityScore()));
						if(cor.getSimilarityScore()>0.0) {
							correspondences.add(cor);
						} 
					}
				}
			}
		}

		// run a maximum weight matching
        MaximumBipartiteMatchingAlgorithm<RecordId,RecordId> max = new MaximumBipartiteMatchingAlgorithm<>(correspondences);
        // max.setVerbose(true);
		max.run();
		correspondences = max.getResult();

		Map<EntityTable, EntityTable> result = new HashMap<>();
		for(Correspondence<RecordId, RecordId> cor : correspondences.get()) {
            EntityTable ent = entityIds.get(cor.getFirstRecord().getIdentifier());
            EntityTable ref = entityIds.get(cor.getSecondRecord().getIdentifier());
            // System.out.println(String.format("Selected mapping between %s and %s for evaluation", ent.getEntityName(), ref.getEntityName(), (int)cor.getSimilarityScore()));
			result.put(ref, ent);
		}

		result.put(referenceNE, detectedNE);

		// return the mapping
		return result;
	}

	public Map<String ,String> mapReferenceToDetectedEntityTableAttributes(
		EntityTable detectedEntityTable,
		EntityTable detectedNonEntityTable,
		EntityTable referenceEntityTable,
		Map<String, Integer> originalWebTableCount
	) {
		Map<String, String> attributeIds = new HashMap<>();

		// assign IDs to the entity tables
		for(String att : detectedEntityTable.getAttributes().keySet()) {
			attributeIds.put(att, att);
		}
		for(String att : detectedNonEntityTable.getAttributes().keySet()) {
			attributeIds.put(att, att);
		}
		for(String att : referenceEntityTable.getAttributes().keySet()) {
			attributeIds.put("reference::" + att, att);
		}

		// create correspondences between attributes
		// - the similarity score is the number of matching input tables
		Processable<Correspondence<RecordId, RecordId>> correspondences = new ParallelProcessableCollection<>();
		for(String att : detectedEntityTable.getAttributes().keySet()) {
			for(String ref : referenceEntityTable.getAttributes().keySet()) {
					Set<String> entProvenance = detectedEntityTable.getAttributeProvenance().get(att);
					Set<String> refProvenance = referenceEntityTable.getAttributeProvenance().get(ref);

					int intersectionSize = getOriginalColumnCount(Q.intersection(entProvenance, refProvenance), originalWebTableCount);

					Correspondence<RecordId, RecordId> cor = new Correspondence<>(new RecordId(att), new RecordId("reference::" + ref), intersectionSize);
                    // System.out.println(String.format("Provenance overlap between %s and %s: %d tables", att, ref, (int)cor.getSimilarityScore()));
                    if(cor.getSimilarityScore()>0.0) {
                        correspondences.add(cor);
                    } 
			}
		}
		for(String att : detectedNonEntityTable.getAttributes().keySet()) {
			for(String ref : referenceEntityTable.getAttributes().keySet()) {
					Set<String> entProvenance = detectedNonEntityTable.getAttributeProvenance().get(att);
					Set<String> refProvenance = referenceEntityTable.getAttributeProvenance().get(ref);

					int intersectionSize = getOriginalColumnCount(Q.intersection(entProvenance, refProvenance), originalWebTableCount);

					Correspondence<RecordId, RecordId> cor = new Correspondence<>(new RecordId(att), new RecordId("reference::" + ref), intersectionSize);
                    // System.out.println(String.format("Provenance overlap between %s and %s: %d tables", att, ref, (int)cor.getSimilarityScore()));
                    if(cor.getSimilarityScore()>0.0) {
                        correspondences.add(cor);
                    } 
			}
		}

		// run a maximum weight matching
        MaximumBipartiteMatchingAlgorithm<RecordId,RecordId> max = new MaximumBipartiteMatchingAlgorithm<>(correspondences);
        // max.setVerbose(true);
		max.run();
		correspondences = max.getResult();

		Map<String, String> result = new HashMap<>();
		for(Correspondence<RecordId, RecordId> cor : correspondences.get()) {
            String att = attributeIds.get(cor.getFirstRecord().getIdentifier());
            String ref = attributeIds.get(cor.getSecondRecord().getIdentifier());
            // System.out.println(String.format("Selected mapping between %s and %s for evaluation", att, ref, (int)cor.getSimilarityScore()));
			result.put(ref, att);
		}

		// return the mapping
		return result;
	}

	private int getOriginalColumnCount(Set<String> ids, Map<String, Integer> originalWebTableCount) {
		int sum = 0;
		for(String id : ids) {
			if(id==null) {
				System.out.println("[EntityTableDefinitionEvalutar.getOriginalColumnCount] Received empty set of column ids!");
			} else {
				Integer value = originalWebTableCount.get(id.split("~")[0]);
				if(value==null) {
					System.out.println(String.format("[EntityTableDefinitionEvaluator.getOriginalColumnCount] No original column count for column with id '%s'!", id));
				} else {
					sum += value.intValue();
				}
			}
		}
		return sum;
	}

	public void evaluateEntityTableDefinition(
		Collection<EntityTable> detectedEntityTables, 
		Collection<EntityTable> referenceEntityTables, 
		File logDirectory, 
		String taskName, 
		SnowPerformance performanceLog,
		Map<String, Integer> originalWebTableCount
	) throws IOException {
		
		System.out.println("Reference Entity Definitions");
		for(EntityTable reference : referenceEntityTables) {
				System.out.println(String.format("\t%s", reference.getEntityName()));
				for(String referenceAttribute : reference.getAttributes().keySet()) {
					System.out.println(String.format("\t\t%s\t%s",
							referenceAttribute,
							StringUtils.join(
									reference.getAttributeProvenance().get(referenceAttribute), 
									",")
							));
				}
		}
		EntityTable detectedNE = null;
		System.out.println("Detected Entity Definitions");
		for(EntityTable detected : detectedEntityTables) {
			System.out.println(String.format("\t%s", detected.getEntityName()));
			for(String detectedAttribute : detected.getAttributes().keySet()) {
				Collection<MatchableTableColumn> detectedColumns = detected.getAttributes().get(detectedAttribute);
				System.out.println(String.format("\t\t%s\t%s",
						detectedAttribute,
						StringUtils.join(
								Q.project(detectedColumns, new MatchableTableColumn.ColumnIdProjection()),
								",")
						));
			}
			if(detected.isNonEntity()) {
				detectedNE = detected;
			}
		}

		Map<EntityTable, EntityTable> tableMapping = mapReferenceToDetectedEntityTables(detectedEntityTables, referenceEntityTables, originalWebTableCount);

		Performance overallPerformance = new Performance(0, 0, 0);
		BigPerformance overallPerformanceOriginal = new BigPerformance(BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO);
		BufferedWriter w = new BufferedWriter(new FileWriter(new File(logDirectory, "entitytable_evaluation.tsv")));

		for(EntityTable reference : referenceEntityTables) {
			EntityTable detected = tableMapping.get(reference);
			Map<String,String> attributeMapping = new HashMap<>();
			if(detected!=null) {
				System.out.println(String.format("[evaluateEntityTableDefinition] Entity table '%s' detected as '%s'", reference.getEntityName(), detected.getEntityName()));
				attributeMapping = mapReferenceToDetectedEntityTableAttributes(detected, detectedNE, reference, originalWebTableCount);

				for(String refAtt : reference.getAttributes().keySet()) {
					String detectedAttribute = attributeMapping.get(refAtt);
					Set<String> refProv = reference.getAttributeProvenance().get(refAtt);
					Set<String> detProv = new HashSet<>();
					if(detectedAttribute!=null) {
						detProv = detected.getAttributeProvenance().get(detectedAttribute);
	
						if(detProv==null) {
							detProv = detectedNE.getAttributeProvenance().get(detectedAttribute);
						}
					}
					Set<String> intersection = Q.intersection(refProv, detProv);
					
					Performance p = new Performance(getOriginalColumnCount(intersection,originalWebTableCount), getOriginalColumnCount(detProv,originalWebTableCount), getOriginalColumnCount(refProv,originalWebTableCount));
					System.out.println(String.format("[evaluateEntityTableDefinition]\tAttribute '%s' detected as '%s' (p: %f / r: %f / f1: %f)", 
						refAtt,
						detectedAttribute,
						p.getPrecision(),
						p.getRecall(),
						p.getF1()
					));
	
					performanceLog.addPerformanceDetail("Entity Table Detection", reference.getEntityName() + "::" + refAtt, p);
	
					overallPerformance = new Performance(
						overallPerformance.getNumberOfCorrectlyPredicted() + p.getNumberOfCorrectlyPredicted(), 
						overallPerformance.getNumberOfPredicted() + p.getNumberOfPredicted(), 
						overallPerformance.getNumberOfCorrectTotal() + p.getNumberOfCorrectTotal());
	
					overallPerformanceOriginal = new BigPerformance(
						overallPerformanceOriginal.getNumberOfCorrectlyPredictedBig().add(
							new BigDecimal(p.getNumberOfCorrectlyPredicted()).multiply(new BigDecimal(p.getNumberOfCorrectlyPredicted()-1.0)).setScale(10).divide(new BigDecimal(2))),
						overallPerformanceOriginal.getNumberOfPredicedBig().add(
							new BigDecimal(p.getNumberOfPredicted()).multiply(new BigDecimal(p.getNumberOfPredicted()-1.0)).setScale(10).divide(new BigDecimal(2))),
						overallPerformanceOriginal.getNumberOfCorrectTotalBig().add(
							new BigDecimal(p.getNumberOfCorrectTotal()).multiply(new BigDecimal(p.getNumberOfCorrectTotal()-1.0)).setScale(10).divide(new BigDecimal(2)))
					);
	
					w.write(String.format("%s\n", StringUtils.join(
						new String[] {
								taskName,
								reference.getEntityName(),
								refAtt,
								detectedAttribute,
								Double.toString(p.getPrecision()),
								Double.toString(p.getRecall()),
								Double.toString(p.getF1()),
								Integer.toString(p.getNumberOfCorrectlyPredicted()),
								Integer.toString(p.getNumberOfPredicted()),
								Integer.toString(p.getNumberOfCorrectTotal())
					}, "\t")));
				}
			}  else {
				System.out.println(String.format("[evaluateEntityTableDefinition] Entity table '%s' missing!", reference.getEntityName()));
			}
		}

		performanceLog.addPerformance("Entity Table Detection", overallPerformance);

		System.out.println(String.format("[evaluateEntityTableDefinition] Overall schema mapping performance p: %f / r: %f / f1: %f",
			overallPerformance.getPrecision(),
			overallPerformance.getRecall(),
			overallPerformance.getF1()
		));

		w.write(String.format("%s\n", StringUtils.join(
				new String[] {
						taskName,
						"overall",
						"",
						"",
						Double.toString(overallPerformance.getPrecision()),
						Double.toString(overallPerformance.getRecall()),
						Double.toString(overallPerformance.getF1()),
						Integer.toString(overallPerformance.getNumberOfCorrectlyPredicted()),
						Integer.toString(overallPerformance.getNumberOfPredicted()),
						Integer.toString(overallPerformance.getNumberOfCorrectTotal()),
						Double.toString(overallPerformanceOriginal.getPrecision()),
						Double.toString(overallPerformanceOriginal.getRecall()),
						Double.toString(overallPerformanceOriginal.getF1()),
						overallPerformanceOriginal.getNumberOfCorrectlyPredictedBig().toString(),
						overallPerformanceOriginal.getNumberOfPredicedBig().toString(),
						overallPerformanceOriginal.getNumberOfCorrectTotalBig().toString()
				}, "\t")));
		
		w.close();
		
	}

	public static void logEntityDefinitions(Collection<EntityTable> entityTables, String taskName, File logDirectory) throws IOException {
		BufferedWriter w = new BufferedWriter(new FileWriter(new File(logDirectory, taskName + "_detected_entity_definition.log")));

		for(EntityTable e : entityTables) {
			for(String a : e.getAttributeProvenance().keySet()) {
				Set<String> prov = e.getAttributeProvenance().get(a);
				w.write(String.format("%s\t%s\t%s\n",
					e.getEntityName(),
					a,
					StringUtils.join(prov, ",")
				));
			}
		}

		w.close();
	}

	public Map<Table, EntityTable> mapUniversalRelationsToReferenceEntityTables(
		StitchedModel universal, 
		Collection<EntityTable> entityTables,
		Map<String, Integer> originalWebTableCount
	) {
		Map<String, Table> tableIds = new HashMap<>();
		Map<String, EntityTable> entityIds = new HashMap<>();

		// assign IDs to the tables
		for(Table t : universal.getTables().values()) {
			tableIds.put(t.getPath(), t);
		}

		// assign IDs to the entity tables
		for(EntityTable ent : entityTables) {
			entityIds.put(ent.getEntityName(), ent);
		}

		// create correspondences between tables and entity tables
		// - the similarity score is the number of matching input tables
		Processable<Correspondence<RecordId, RecordId>> correspondences = new ParallelProcessableCollection<>();
		for(Table t : universal.getTables().values()) {
			for(EntityTable ent : entityTables) {
                if(!ent.isNonEntity()) {
                    Set<String> tableProvenance = t.getProvenance();
                    Set<String> entProvenance = ent.getProvenance();

					int intersectionSize = getOriginalColumnCount(Q.intersection(tableProvenance, entProvenance), originalWebTableCount);

                    Correspondence<RecordId, RecordId> cor = new Correspondence<>(new RecordId(t.getPath()), new RecordId(ent.getEntityName()), intersectionSize);
                    System.out.println(String.format("Provenance overlap between #%d and %s: %d tables", t.getTableId(), ent.getEntityName(), (int)cor.getSimilarityScore()));
                    if(cor.getSimilarityScore()>0.0) {
                        correspondences.add(cor);
                    } 
                }
			}
		}

		// run a maximum weight matching
        MaximumBipartiteMatchingAlgorithm<RecordId,RecordId> max = new MaximumBipartiteMatchingAlgorithm<>(correspondences);
        // max.setVerbose(true);
		max.run();
		correspondences = max.getResult();

		Map<Table, EntityTable> result = new HashMap<>();
		for(Correspondence<RecordId, RecordId> cor : correspondences.get()) {
            Table t = tableIds.get(cor.getFirstRecord().getIdentifier());
            EntityTable ent = entityIds.get(cor.getSecondRecord().getIdentifier());
            System.out.println(String.format("Selected mapping between #%d and %s for evaluation", t.getTableId(), ent.getEntityName(), (int)cor.getSimilarityScore()));
			result.put(t, ent);
		}

		// return the mapping
		return result;
	}

	public List<Pair<Table, EntityTable>> mapAllUniversalRelationsToReferenceEntityTables(
		StitchedModel universal, 
		Collection<EntityTable> entityTables,
		Map<String, Integer> originalWebTableCount
	) {
		Map<String, Table> tableIds = new HashMap<>();
		Map<String, EntityTable> entityIds = new HashMap<>();

		// assign IDs to the tables
		for(Table t : universal.getTables().values()) {
			tableIds.put(t.getPath(), t);
		}

		// assign IDs to the entity tables
		for(EntityTable ent : entityTables) {
			entityIds.put(ent.getEntityName(), ent);
		}

		// create correspondences between tables and entity tables
		// - the similarity score is the number of matching input tables
		Processable<Correspondence<RecordId, RecordId>> correspondences = new ParallelProcessableCollection<>();
		for(Table t : universal.getTables().values()) {
			for(EntityTable ent : entityTables) {
                if(!ent.isNonEntity()) {
                    Set<String> tableProvenance = t.getProvenance();
                    Set<String> entProvenance = ent.getProvenance();

					int intersectionSize = getOriginalColumnCount(Q.intersection(tableProvenance, entProvenance), originalWebTableCount);

                    Correspondence<RecordId, RecordId> cor = new Correspondence<>(new RecordId(t.getPath()), new RecordId(ent.getEntityName()), intersectionSize);
                    System.out.println(String.format("Provenance overlap between #%d and %s: %d tables", t.getTableId(), ent.getEntityName(), (int)cor.getSimilarityScore()));
                    if(cor.getSimilarityScore()>0.0) {
                        correspondences.add(cor);
                    } 
                }
			}
		}

		// run a maximum weight matching
		MatchingEngine<RecordId, RecordId> engine = new MatchingEngine<>();
		correspondences = engine.getTopKInstanceCorrespondences(correspondences, 1, 0.0);

		List<Pair<Table, EntityTable>> result = new LinkedList<>();
		for(Correspondence<RecordId, RecordId> cor : correspondences.sort((c)->c.getSimilarityScore(), false).get()) {
            Table t = tableIds.get(cor.getFirstRecord().getIdentifier());
            EntityTable ent = entityIds.get(cor.getSecondRecord().getIdentifier());
            System.out.println(String.format("Selected mapping between #%d and %s for evaluation", t.getTableId(), ent.getEntityName(), (int)cor.getSimilarityScore()));
			result.add(new Pair<>(t, ent));
		}

		// return the mapping
		return result;
	}

	public Map<String, TableColumn> mapUniversalRelationsAttributesToReferenceEntityTables(
		Table universal,
		EntityTable referenceEntityTable,
		EntityTable nonEntityTable,
		Map<String, Integer> originalWebTableCount,
		boolean verbose
	) {
		Map<String, String> attributeIds = new HashMap<>();
		Map<String, TableColumn> columnIds = new HashMap<>();

		// assign IDs to the entity tables
		for(TableColumn att : universal.getColumns()) {
			if(!"FK".equals(att.getHeader())) {
				columnIds.put(att.getIdentifier(), att);
			}
		}
		for(String att : referenceEntityTable.getAttributes().keySet()) {
			attributeIds.put("reference::" + att, att);
		}
		for(String att : nonEntityTable.getAttributes().keySet()) {
			attributeIds.put("ne::" + att, att);
		}

		// create correspondences between attributes
		// - the similarity score is the number of matching input tables
		Processable<Correspondence<RecordId, RecordId>> correspondences = new ParallelProcessableCollection<>();
		for(TableColumn att : universal.getColumns()) {
			System.out.println(String.format("%s: %s",
				att.getHeader(),
				StringUtils.join(att.getProvenance(), ",")
			));
			if(!"FK".equals(att.getHeader())) {
				for(String ref : referenceEntityTable.getAttributes().keySet()) {
					Set<String> entProvenance = new HashSet<>(att.getProvenance());
					Set<String> refProvenance = referenceEntityTable.getAttributeProvenance().get(ref);

					int intersectionSize = getOriginalColumnCount(Q.intersection(entProvenance, refProvenance), originalWebTableCount);

					Correspondence<RecordId, RecordId> cor = new Correspondence<>(new RecordId(att.getIdentifier()), new RecordId("reference::" + ref), intersectionSize);
					// System.out.println(String.format("Provenance overlap between %s and %s: %d tables", att, ref, (int)cor.getSimilarityScore()));
					if(cor.getSimilarityScore()>0.0) {
						correspondences.add(cor);

						if(verbose) {
							System.out.println(String.format("\t%d\t%s -> %s",
								intersectionSize,
								att.toString(),
								ref
							));
						}
					} 
				}
				for(String ref : nonEntityTable.getAttributes().keySet()) {
					Set<String> entProvenance = new HashSet<>(att.getProvenance());
					Set<String> refProvenance = nonEntityTable.getAttributeProvenance().get(ref);

					int intersectionSize = getOriginalColumnCount(Q.intersection(entProvenance, refProvenance), originalWebTableCount);

					Correspondence<RecordId, RecordId> cor = new Correspondence<>(new RecordId(att.getIdentifier()), new RecordId("ne::" + ref), intersectionSize);
					// System.out.println(String.format("Provenance overlap between %s and %s: %d tables", att, ref, (int)cor.getSimilarityScore()));
					if(cor.getSimilarityScore()>0.0) {
						correspondences.add(cor);

						if(verbose) {
							System.out.println(String.format("\t%d\t%s -> %s",
								intersectionSize,
								att.toString(),
								ref
							));
						}
					} 
				}
			}
		}

		// run a maximum weight matching
        MaximumBipartiteMatchingAlgorithm<RecordId,RecordId> max = new MaximumBipartiteMatchingAlgorithm<>(correspondences);
        // max.setVerbose(true);
		max.run();
		correspondences = max.getResult();

		Map<String, TableColumn> result = new HashMap<>();
		for(Correspondence<RecordId, RecordId> cor : correspondences.get()) {
            TableColumn att = columnIds.get(cor.getFirstRecord().getIdentifier());
            String ref = attributeIds.get(cor.getSecondRecord().getIdentifier());
            // System.out.println(String.format("Selected mapping between %s and %s for evaluation", att, ref, (int)cor.getSimilarityScore()));
			result.put(ref, att);
		}

		// return the mapping
		return result;
	}

	public Map<String, TableColumn> mapAttributesToReferenceEntityTables(
		Collection<TableColumn> attributes,
		EntityTable referenceEntityTable,
		EntityTable nonEntityTable,
		Set<String> referenceAttributesToMap,
		Map<String, Integer> originalWebTableCount,
		boolean verbose
	) {
		Map<String, String> attributeIds = new HashMap<>();
		Map<String, TableColumn> columnIds = new HashMap<>();

		// assign IDs to the entity tables
		for(TableColumn att : attributes) {
			if(!"FK".equals(att.getHeader())) {
				columnIds.put(att.getIdentifier(), att);
			}
		}

		for(String att : referenceEntityTable.getAttributes().keySet()) {
			if(referenceAttributesToMap.contains(att)) {
				attributeIds.put("reference::" + att, att);
			}
		}
		for(String att : nonEntityTable.getAttributes().keySet()) {
			if(referenceAttributesToMap.contains(att)) {
				attributeIds.put("ne::" + att, att);
			}
		}

		// create correspondences between attributes
		// - the similarity score is the number of matching input tables
		Processable<Correspondence<RecordId, RecordId>> correspondences = new ParallelProcessableCollection<>();
		for(TableColumn att : attributes) {
			if(verbose) System.out.println(String.format("%s: %s",
				att.getHeader(),
				StringUtils.join(att.getProvenance(), ",")
			));
			if(!"FK".equals(att.getHeader())) {
				for(String ref : referenceEntityTable.getAttributes().keySet()) {
					if(referenceAttributesToMap.contains(ref)) {
						Set<String> entProvenance = new HashSet<>(att.getProvenance());
						Set<String> refProvenance = referenceEntityTable.getAttributeProvenance().get(ref);

						int intersectionSize = getOriginalColumnCount(Q.intersection(entProvenance, refProvenance), originalWebTableCount);

						Correspondence<RecordId, RecordId> cor = new Correspondence<>(new RecordId(att.getIdentifier()), new RecordId("reference::" + ref), intersectionSize);
						// System.out.println(String.format("Provenance overlap between %s and %s: %d tables", att, ref, (int)cor.getSimilarityScore()));
						if(cor.getSimilarityScore()>0.0) {
							correspondences.add(cor);

							if(verbose) {
								System.out.println(String.format("\t%d\t%s -> %s",
									intersectionSize,
									att.toString(),
									ref
								));
							}
						} 
					}
				}
				for(String ref : nonEntityTable.getAttributes().keySet()) {
					if(referenceAttributesToMap.contains(ref)) {
						Set<String> entProvenance = new HashSet<>(att.getProvenance());
						Set<String> refProvenance = nonEntityTable.getAttributeProvenance().get(ref);

						int intersectionSize = getOriginalColumnCount(Q.intersection(entProvenance, refProvenance), originalWebTableCount);

						Correspondence<RecordId, RecordId> cor = new Correspondence<>(new RecordId(att.getIdentifier()), new RecordId("ne::" + ref), intersectionSize);
						// System.out.println(String.format("Provenance overlap between %s and %s: %d tables", att, ref, (int)cor.getSimilarityScore()));
						if(cor.getSimilarityScore()>0.0) {
							correspondences.add(cor);

							if(verbose) {
								System.out.println(String.format("\t%d\t%s -> %s",
									intersectionSize,
									att.toString(),
									ref
								));
							}
						} 
					}
				}
			}
		}

		// run a maximum weight matching
        MaximumBipartiteMatchingAlgorithm<RecordId,RecordId> max = new MaximumBipartiteMatchingAlgorithm<>(correspondences);
        // max.setVerbose(true);
		max.run();
		correspondences = max.getResult();

		Map<String, TableColumn> result = new HashMap<>();
		for(Correspondence<RecordId, RecordId> cor : correspondences.get()) {
            TableColumn att = columnIds.get(cor.getFirstRecord().getIdentifier());
            String ref = attributeIds.get(cor.getSecondRecord().getIdentifier());
			// System.out.println(String.format("Selected mapping between %s and %s for evaluation", att, ref, (int)cor.getSimilarityScore()));
			result.put(ref, att);

			if(verbose) {
				System.out.println(String.format("Selected mapping %s -> %s (%d)", att, ref, (int)cor.getSimilarityScore()));
			}
		}

		// return the mapping
		return result;
	}

	// public Pair<Pair<Set<TableColumn>, Set<TableColumn>>, Map<String, TableColumn>> getBestMatchingFD(
	// 	Collection<Pair<Set<TableColumn>,Set<TableColumn>>> existingFDs, 
	// 	Pair<Set<String>,Set<String>> fdToEvaluate,
	// 	EntityTable referenceEntityTable,
	// 	EntityTable nonEntityTable,
	// 	Map<String, Integer> originalWebTableCount
	// ) {
	// 	Map<String, TableColumn> mapping = null;
	// 	Pair<Set<TableColumn>, Set<TableColumn>> bestMatch = null;
	// 	int maxOverlap = 0;
	// 	int maxSchemaOverlap = 1;
	// 	// System.out.println(String.format("Mapping columns for FD {%s}->{%s}",
	// 	// 	StringUtils.join(fdToEvaluate.getFirst(), ","),
	// 	// 	StringUtils.join(fdToEvaluate.getSecond(), ",")
	// 	// ));
	// 	for(Pair<Set<TableColumn>, Set<TableColumn>> existingFD : existingFDs) {

	// 		if(
	// 			existingFD.getFirst().size()<=fdToEvaluate.getFirst().size()+1	// +1 because we removed FK from the FD to evaluate!
	// 			&&
	// 			Q.any(existingFD.getFirst(), (c)->"FK".equals(c.getHeader()))
	// 		) { 
	// 			// map the FD to the evaluated FD
	// 			Map<String, TableColumn> m = mapAttributesToReferenceEntityTables(
	// 				Q.union(existingFD.getFirst(), existingFD.getSecond()), 
	// 				referenceEntityTable, 
	// 				nonEntityTable, 
	// 				Q.union(fdToEvaluate.getFirst(), fdToEvaluate.getSecond()),
	// 				originalWebTableCount, 
	// 				false);

	// 			// check if it is a match for the determinant
	// 			// - there must be no additional attribute in the determinant
	// 			int schemaOverlap = 0;
	// 			Map<TableColumn, String> i = MapUtils.invert(m);
	// 			for(TableColumn c : existingFD.getFirst()) {
	// 				if("FK".equals(c.getHeader()) || i.containsKey(c) && fdToEvaluate.getFirst().contains(i.get(c))) {
	// 					schemaOverlap++;
	// 				} else {
	// 					schemaOverlap = 0;
	// 					break;
	// 				}
	// 			}

	// 			if(schemaOverlap>=maxSchemaOverlap) {
	// 				// measure the overlap score
	// 				int overlap = referenceEntityTable.getNumberOfMatchedColumns(m.values(), Q.union(fdToEvaluate.getFirst(), fdToEvaluate.getSecond()), nonEntityTable, originalWebTableCount);

	// 				if(overlap>0) {
	// 					// System.out.println(String.format("\tMatch with score %d: {%s}->{%s}",
	// 					// 	overlap,
	// 					// 	StringUtils.join(Q.project(existingFD.getFirst(), (c)->c.toString()), ","),
	// 					// 	StringUtils.join(Q.project(existingFD.getSecond(), (c)->c.toString()), ",")
	// 					// ));
	// 				}

	// 				// choose the FD with the highest overlap
	// 				if(overlap>maxOverlap || schemaOverlap>maxSchemaOverlap) {
	// 					mapping = m;
	// 					bestMatch = existingFD;
	// 					maxOverlap = overlap;
	// 					maxSchemaOverlap = schemaOverlap;
	// 				}
	// 			} else {
	// 				// System.out.println(String.format("\tNo Match: {%s} -> {%s}",
	// 				// 	StringUtils.join(Q.project(existingFD.getFirst(), (c)->c.toString()), ","),
	// 				// 	StringUtils.join(Q.project(existingFD.getSecond(), (c)->c.toString()), ",")
	// 				// ));
	// 			}
	// 		}
	// 	}

	// 	return new Pair<>(bestMatch, mapping);
	// }

	public Pair<Pair<Set<TableColumn>, Set<TableColumn>>, Map<String, TableColumn>> getBestMatchingFD(
		Collection<Pair<Set<TableColumn>,Set<TableColumn>>> existingFDs, 
		Pair<Set<String>,Set<String>> fdToEvaluate,
		EntityTable referenceEntityTable,
		EntityTable nonEntityTable,
		Map<String, Integer> originalWebTableCount,
		Set<TableColumn> evaluatedDependants
	) {
		Map<String, TableColumn> mapping = null;
		Pair<Set<TableColumn>, Set<TableColumn>> bestMatch = null;
		int maxOverlap = 0;
		int maxSchemaOverlap = 1;
		int maxExtraColumns = Integer.MAX_VALUE;
		// System.out.println(String.format("Mapping columns for FD {%s}->{%s}",
		// 	StringUtils.join(fdToEvaluate.getFirst(), ","),
		// 	StringUtils.join(fdToEvaluate.getSecond(), ",")
		// ));
		for(Pair<Set<TableColumn>, Set<TableColumn>> existingFD : existingFDs) {

			if(
				//existingFD.getFirst().size()<=fdToEvaluate.getFirst().size()+1	// +1 because we removed FK from the FD to evaluate!
				//&&
				Q.any(existingFD.getFirst(), (c)->"FK".equals(c.getHeader()))
			) { 
				// map the FD to the evaluated FD
				Map<String, TableColumn> m = mapAttributesToReferenceEntityTables(
					Q.union(existingFD.getFirst(), existingFD.getSecond()), 
					referenceEntityTable, 
					nonEntityTable, 
					Q.union(fdToEvaluate.getFirst(), fdToEvaluate.getSecond()),
					originalWebTableCount, 
					false);
				Map<TableColumn, String> i = MapUtils.invert(m);

				// check if it is a match for the dependant
				TableColumn dependantMatch = null;
				for(TableColumn c : existingFD.getSecond()) {
					if(i.containsKey(c) && fdToEvaluate.getSecond().contains(i.get(c)) && !evaluatedDependants.contains(c)) {
						dependantMatch = c;
						break;
					}
				}

				// check if it is a match for the determinant
				int schemaOverlap = 0;
				int extraColumns = 0;
				Boolean isTrivial = false;
				for(TableColumn c : existingFD.getFirst()) {
					if("FK".equals(c.getHeader()) || i.containsKey(c) && fdToEvaluate.getFirst().contains(i.get(c))) {
						schemaOverlap++;
					} else {
						extraColumns++;
					}
					// make sure the dependant is not part of the determinant (trivial FD)
					if(c.equals(dependantMatch)) {
						isTrivial = true;
					}
				}

				if(dependantMatch!=null && !isTrivial && schemaOverlap>=maxSchemaOverlap) {
					// measure the overlap score
					int overlap = referenceEntityTable.getNumberOfMatchedColumns(m.values(), Q.union(fdToEvaluate.getFirst(), fdToEvaluate.getSecond()), nonEntityTable, originalWebTableCount);

					// DEBUG
					if("price".equals(Q.firstOrDefault(fdToEvaluate.getSecond()))) {
						System.out.println(String.format("Match {%s}->{%s}; schema overlap: %d; extra attributes: %d; column overlap: %d", 
							StringUtils.join(Q.project(existingFD.getFirst(), (c)->c.getHeader()), ","),
							StringUtils.join(Q.project(existingFD.getSecond(), (c)->c.getHeader()), ","),
							schemaOverlap,
							extraColumns,
							overlap));
					}

					if(overlap>0) {
						// System.out.println(String.format("\tMatch with score %d: {%s}->{%s}",
						// 	overlap,
						// 	StringUtils.join(Q.project(existingFD.getFirst(), (c)->c.toString()), ","),
						// 	StringUtils.join(Q.project(existingFD.getSecond(), (c)->c.toString()), ",")
						// ));
					}

					// choose the FD with the highest overlap
					//if(overlap>maxOverlap || schemaOverlap>maxSchemaOverlap) {
						//TODO: check if condition with extraColumns has influence on any other dataset!
					if((overlap>maxOverlap || schemaOverlap>maxSchemaOverlap) && extraColumns<=maxExtraColumns) {
						mapping = m;
						bestMatch = existingFD;
						maxOverlap = overlap;
						maxSchemaOverlap = schemaOverlap;
						extraColumns = maxExtraColumns;
					}
				} else {
					// System.out.println(String.format("\tNo Match: {%s} -> {%s}",
					// 	StringUtils.join(Q.project(existingFD.getFirst(), (c)->c.toString()), ","),
					// 	StringUtils.join(Q.project(existingFD.getSecond(), (c)->c.toString()), ",")
					// ));
				}
			}
		}

		return new Pair<>(bestMatch, mapping);
	}

	public Map<EntityTable, Collection<Table>> evaluateFunctionalDependencies(
		StitchedModel universal, 
        Collection<EntityTable> referenceEntityTables,
		Map<String, Table> entityIdToEntityTable,
        String taskName,
        File logDirectory,
        boolean heuristicDependencyPropagation,
        boolean propagateDependencies,
        boolean assumeBinary,
		boolean discoverFDs,
		SnowPerformance performanceLog,
		Map<String, Integer> originalWebTableCount,
		String configuration,
		String step
	) throws Exception {
		File logFile = new File(logDirectory, step + "functional_dependency_evaluation.log");
		File resultsFile = new File(logDirectory.getParentFile(), step + "functional_dependency_evaluation.tsv");
		File detailsFile = new File(logDirectory.getParentFile(), step + "functional_dependency_evaluation_details.tsv");
		BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile));
		BufferedWriter resultsWriter = new BufferedWriter(new FileWriter(resultsFile, true));
		BufferedWriter detailsWriter = new BufferedWriter(new FileWriter(detailsFile, true));
		
		Map<EntityTable, Collection<Table>> relationsForFDs = new HashMap<>();

		// find the 'ne' table that contains all attribute definitions for non-key attributes
		// (only exists if entity structure was loaded from files, not available after matching)
		EntityTable nonEntityTable = null;
        for(EntityTable et : referenceEntityTables) {
			if(et.isNonEntity()) {
				nonEntityTable = et;
				break;
			}
		}

		int ttlSum = 0;
		int inClosureSum = 0;
		double corSum = 0;
		int corAbsSum = 0;
		double weightedByColumnsSum = 0;
		int weightedByColumnsAbsSum = 0;
		int weightedByColumnsInClosureSum = 0;
		int totalColumnsSum = 0;

		// create a 1:1 mapping from universal relations to the reference entity tables
		// - a 1:1 mapping gives incorrect results if FD sttiching is deactivated (then there can be multiple schemata for each reference table)
		// Map<Table, EntityTable> entityMapping = mapUniversalRelationsToReferenceEntityTables(universal, referenceEntityTables, originalWebTableCount);
		// rather: 
		// -- map every table to a reference table
		// -- sort tables by overlap and evaluate in order (so the table with the largest overlap is evaluated first)
		// -- remove FDs from evaluation set after they have been found (so they are not re-evaluated for a table with smaller overlap)
		List<Pair<Table, EntityTable>> tableMapping = mapAllUniversalRelationsToReferenceEntityTables(universal, referenceEntityTables, originalWebTableCount);

		// keep track of FDs which were already evaluated
		Map<EntityTable, Map<Set<String>,Set<String>>> evaluatedFDs = new HashMap<>();

		// for(Table t : universal.getTables().values()) {
		for(Pair<Table, EntityTable> tableCor : tableMapping) {
			Table t = tableCor.getFirst();

			Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies = t.getSchema().getFunctionalDependencies();
			Collection<Pair<Set<TableColumn>, Set<TableColumn>>> allClosures = Pair.fromMap(StitchedFunctionalDependencyUtils.allClosures(t.getSchema().getFunctionalDependencies(), universal));

			// get the corresponding entity table
			TableColumn foreignKey = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));
			// Object firstFKValue = null;
			// for(TableRow r : t.getRows()) {
			// 	firstFKValue = r.get(foreignKey.getColumnIndex());
			// 	if(firstFKValue!=null) {
			// 		break;
			// 	}
			// }
			// EntityTable ent = null;
			// if(firstFKValue!=null) {
			// 	Table et = entityIdToEntityTable.get(firstFKValue);
			// 	ent = entityTables.get(et);
			// }
			// EntityTable ent = entityMapping.get(t);
			EntityTable ent = tableCor.getSecond();
			if(ent==null) {
				continue;
			}

			if(!evaluatedFDs.containsKey(ent)) {
				evaluatedFDs.put(ent, new HashMap<>());
			}

			System.out.println(String.format("[EvaluateFunctionalDependencies] Evaluating entity type '%s'",ent.getEntityName()));
			System.out.println(String.format("[EvaluateFunctionalDependencies] Universal relation is {%s}", StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ",")));
			logWriter.write(String.format("Entity type '%s': {%s}\n",
				ent.getEntityName(),
				StringUtils.join(Q.project(t.getColumns(), (c)->c.toString()), ",")
			));


			int ttl = 0;
			int inClosure = 0;
			int weightedByColumnsAbs = 0;
			double weightedByColumns = 0;
			int weightedByColumnsInClosure = 0;
			int totalColumns = 0;
			double cor = 0;
			int corAbs = 0;
			// evaluate the FDs
			List<Pair<Set<String>,Set<String>>> fdsToEvaluate = new ArrayList<>(Pair.fromMap(ent.getFunctionalDependencies()));
			// sort FDs so we can use the created file name to match extracted relations and reference relations
			Collections.sort(fdsToEvaluate, (p1,p2)->
				StringUtils.join(Q.union(p1.getFirst(), p1.getSecond()), "")
				.compareTo(
					StringUtils.join(Q.union(p2.getFirst(), p2.getSecond()), "")
				)
			);

			int totalFDsToEvaluate = 0;
			for(Pair<Set<String>, Set<String>> fdToCheck : fdsToEvaluate) {
				totalFDsToEvaluate += fdToCheck.getSecond().size();
			}

			int fdIndex = 0;
			for(Pair<Set<String>, Set<String>> fdToCheck : fdsToEvaluate) {
				ttl+=fdToCheck.getSecond().size();
				int columnsInFD = ent.getNumberOfOriginalColumnsForRelation(Q.union(fdToCheck.getFirst(), fdToCheck.getSecond()), nonEntityTable, originalWebTableCount);
				// totalColumns+= columnsInFD;

				System.out.println(String.format("[EvaluateFunctionalDependencies] evaluating {%s}->{%s} (%d original columns)",
						StringUtils.join(fdToCheck.getFirst(), ","),
						StringUtils.join(fdToCheck.getSecond(), ","),
						columnsInFD
				));

				Set<String> determinantToCheck = new HashSet<>(fdToCheck.getFirst());

				Set<TableColumn> extraColumns = new HashSet<>();
				if(determinantToCheck.contains("FK")) {
					determinantToCheck.remove("FK");
					extraColumns.add(foreignKey);
				}

				// sort dependants by decreasing size of original columns
				// - this way, if attributes are incorrectly merged, we will always evaluate the one with the largest overlap and skip the rest
				List<String> dependants = ent.sortByNumberOfOriginalColumns(fdToCheck.getSecond(), nonEntityTable, originalWebTableCount);

				// keep track of all attributes which have been evaluated as dependant
				// - if columns were incorrectly matched, two different attributes in the dependant of the FD in the gold standard can refer to the same attribute in the universal relation
				// - in such a case, we add the score only once!
				Set<TableColumn> evaluatedDependants = new HashSet<>();

				if(!evaluatedFDs.get(ent).containsKey(fdToCheck.getFirst())) {
					evaluatedFDs.get(ent).put(fdToCheck.getFirst(), new HashSet<>());
				}

				// evaluation for matching results: if we have two different clusters corresponding to the same attribute
				// - we cannot create a single 1:1 mapping to evaluate all FDs
				// - if only one cluster is contained in a correctly detected an FD and we choose the other one, we will consider the FD as not detected
				// - hence we have to map the clusters to attributes for each evaluated FD individually
				// create all extended minimal FDs
				// - check for each of them if the attributes in the determinant and dependant are a possible match for the evaluated FD
				// - then create a 1:1 mapping for all matching clusters
				// - finally, select the extended FD & mapping with the highest overlap
				for(String dependantAttribute : dependants) {
					if(!evaluatedFDs.get(ent).get(fdToCheck.getFirst()).contains(dependantAttribute)) {
						int weightedByColumnsAbsDep = 0;
						double weightedByColumnsDep = 0;
						int weightedByColumnsInClosureDep = 0;
						int totalColumnsDep = 0;
						int ttlDep = 1;
						int inClosureDep = 0;
						double corDep = 0;
						int corAbsDep = 0;

						String fdName = String.format("%s: {%s}->{%s}", ent.getEntityName(), StringUtils.join(fdToCheck.getFirst(), ","), dependantAttribute);

						int columnsInFdForDependant = ent.getNumberOfOriginalColumnsForRelation(Q.toSet(dependantAttribute), nonEntityTable, originalWebTableCount);
						totalColumns += columnsInFdForDependant;
						totalColumnsDep += columnsInFdForDependant;

						Set<TableColumn> dependant = new HashSet<>();

						Pair<Pair<Set<TableColumn>, Set<TableColumn>>, Map<String, TableColumn>> bestMatch = getBestMatchingFD(
							allClosures, 
							new Pair<>(determinantToCheck, Q.toSet(dependantAttribute)), 
							ent, 
							nonEntityTable, 
							originalWebTableCount,
							evaluatedDependants);
						Pair<Set<TableColumn>, Set<TableColumn>> bestFD = bestMatch.getFirst();
						Map<String, TableColumn> fdMapping = bestMatch.getSecond();
						Map<TableColumn, String> inverseFdMapping = null;
						TableColumn depCol = null;
						if(fdMapping!=null) {
							inverseFdMapping = MapUtils.invert(fdMapping);
							// for(String a : fdMapping.keySet()) {
							// 	System.out.println(String.format("\t%s -> %s",
							// 		a,
							// 		fdMapping.get(a)
							// 	));
							// }

							depCol = fdMapping.get(dependantAttribute);
							if(depCol!=null) {
								dependant.add(depCol);
							}
						}

						if(bestFD!=null) {
							System.out.println(String.format("[EvaluateFunctionalDependencies]\tTranslated FD %s to {%s}->{%s}",
								fdName,
								StringUtils.join(Q.project(bestFD.getFirst(), (c)->c.toString()), ","),
								StringUtils.join(Q.project(dependant, (c)->c.toString()), ",")
							));
						} 

						if(depCol!=null && evaluatedDependants.contains(depCol)) {
							System.out.println(String.format("[EvaluateFunctionalDependencies]\tDependant for {%s}->{%s} (%d original columns) (represented by '%s') already evaluated for another dependant!",
								StringUtils.join(fdToCheck.getFirst(), ","),
								dependantAttribute,
								columnsInFdForDependant,
								dependant.toString()
							));
						} else {
							evaluatedDependants.add(depCol);
							if(dependant.size()==0) {
								System.out.println(String.format("[EvaluateFunctionalDependencies]\tCould not translate dependant: {%s}->{%s}",
									StringUtils.join(fdToCheck.getFirst(), ","),
									dependantAttribute
								));
							} else {

								// weight the FD only by the dependant attribute
								// - if we took the determinant into account, the weight could be biased towards its columns
								// - example: determinant occurs in 100 tables, but dependant only in 1
								// --- if we weight by determinant, the FD is considered to appear in all 100 tables, which is wrong
								int numMatchedColumns = ent.getNumberOfMatchedColumns(Q.union(bestFD.getFirst(), bestFD.getSecond()), Q.toSet(dependantAttribute), nonEntityTable, originalWebTableCount);

								if(bestFD!=null && depCol!=null) {
									inClosure++;
									inClosureDep++;
									weightedByColumnsInClosure+=numMatchedColumns;
									weightedByColumnsInClosureDep+=numMatchedColumns;

									Pair<Set<TableColumn>,Set<TableColumn>> reduced = StitchedFunctionalDependencyUtils.reduceLHS(new Pair<>(bestFD.getFirst(), Q.toSet(depCol)), functionalDependencies, universal, false, false);

									Set<TableColumn> matchedDeterminantColumns = Q.intersection(reduced.getFirst(), fdMapping.values());

									//if(reduced.getFirst().size()==fdToCheck.getFirst().size()) {
									if(matchedDeterminantColumns.size()==fdToCheck.getFirst().size()) {
										cor++;
										corAbs++;
										corDep++;
										corAbsDep++;
										weightedByColumns+=numMatchedColumns;
										weightedByColumnsAbs+=numMatchedColumns;
										weightedByColumnsDep+=numMatchedColumns;
										weightedByColumnsAbsDep+=numMatchedColumns;

										System.out.println(String.format("[EvaluateFunctionalDependencies]\t[+1.00] found FD: {%s}->{%s} (%d/%d original columns)",
											StringUtils.join(Q.project(bestFD.getFirst(), (c)->c.getHeader()), ","),
											depCol.getHeader(),
											numMatchedColumns,
											columnsInFdForDependant
										));
										logWriter.write(String.format("found directly: {%s}->{%s}\n",
											StringUtils.join(fdToCheck.getFirst(), ","),
											depCol.getHeader()	
										));
									} else {
										//double score = reduced.getFirst().size()/(double)fdToCheck.getFirst().size();

										//TODO: this score doesn't make much sense ...
										double score = matchedDeterminantColumns.size()/(double)fdToCheck.getFirst().size() - reduced.getFirst().size()/(double)fdToCheck.getFirst().size();
										
										Set<String> reducedRelation = new HashSet<>();
										for(TableColumn c : reduced.getFirst()) {
											if("FK".equals(c.getHeader())) {
												reducedRelation.add("FK");
											} else {
												reducedRelation.add(inverseFdMapping.get(c));
											}
										}
										weightedByColumns+= numMatchedColumns * score;
										weightedByColumnsDep+= numMatchedColumns * score;

										if(matchedDeterminantColumns.size()>fdToCheck.getFirst().size()) {
											System.out.println(String.format("[EvaluateFunctionalDependencies]\t [+%.2f] found FD: {%s}+{%s}->{%s} (%d/%d original columns) corresponding to {%s}",
												score,
												StringUtils.join(Q.project(reduced.getFirst(), (c)->c.getHeader()), ","),
												StringUtils.join(Q.project(Q.without(matchedDeterminantColumns,reduced.getFirst()), (c)->c.getHeader()), ","),
												depCol.getHeader(),
												numMatchedColumns,
												columnsInFdForDependant,
												StringUtils.join(reducedRelation, ",")
											));
										} else {
											System.out.println(String.format("[EvaluateFunctionalDependencies]\t [+%.2f] found FD: {%s}->...->{%s} (%d/%d original columns) corresponding to {%s}",
												score,
												StringUtils.join(Q.project(reduced.getFirst(), (c)->c.getHeader()), ","),
												depCol.getHeader(),
												numMatchedColumns,
												columnsInFdForDependant,
												StringUtils.join(reducedRelation, ",")
											));
										}

										cor += score;
										corDep += score;

										logWriter.write(String.format("found in closure: {%s}->{%s}\n",
											StringUtils.join(fdToCheck.getFirst(), ","),
											depCol.getHeader()	
										));
									}

									// we have at least a partial match, project the FD into a new relation for value-based evaluation
									Collection<Table> relationsForEnt = MapUtils.get(relationsForFDs, ent, new LinkedList<>());
									Table fdRel = extractRelationForFD(new Pair<>(reduced.getFirst(), dependant), t, inverseFdMapping, ent, nonEntityTable);
									fdRel.setTableId(fdIndex);
									fdRel.setPath(String.format("%s_%s_fd_%d", ent.getEntityName(), taskName, fdRel.getTableId()));
									relationsForEnt.add(fdRel);

									evaluatedFDs.get(ent).get(fdToCheck.getFirst()).add(dependantAttribute);
								} else {
									System.out.println(String.format("[EvaluateFunctionalDependencies]\tmissing FD: {%s}->{%s}",
										StringUtils.join(fdToCheck.getFirst(), ","),
										depCol.getHeader()
									));
									logWriter.write(String.format("not found: {%s}->{%s}\n",
										StringUtils.join(fdToCheck.getFirst(), ","),
										depCol.getHeader()	
									));
								}
							}
						}

						Performance columnsStrict = new Performance(weightedByColumnsAbsDep, weightedByColumnsAbsDep, totalColumnsDep);
						Performance columnsWeighted = new IRPerformance(1.0, weightedByColumnsDep / (double)totalColumnsDep);
						Performance columnsImplied = new Performance(weightedByColumnsInClosureDep, weightedByColumnsInClosureDep, totalColumnsDep);
						performanceLog.addPerformanceDetail(step + "FD Stitching (strict)", fdName, new Performance(corAbsDep, corAbsDep, ttlDep));
						performanceLog.addPerformanceDetail(step + "FD Stitching (weighted)", fdName, new IRPerformance(1.0, corDep / (double)ttlDep));
						performanceLog.addPerformanceDetail(step + "FD Stitching (in closure)", fdName, new Performance(inClosureDep, inClosureDep, ttlDep));
						performanceLog.addPerformanceDetail(step + "FD Stitching (matched columns strict)", fdName, columnsStrict);
						performanceLog.addPerformanceDetail(step + "FD Stitching (matched columns weighted)", fdName, columnsWeighted);
						performanceLog.addPerformanceDetail(step + "FD Stitching (matched columns in closure)", fdName, columnsImplied);

						int valuesForDepInUniversal = 0;
						if(depCol!=null) {
							for(TableRow r : t.getRows()) {
								if(r.get(depCol.getColumnIndex())!=null) {
									valuesForDepInUniversal++;
								}
							}
						}

						detailsWriter.write(String.format("%s\n", StringUtils.join(new String[] {
							taskName,
							configuration,
							ent.getEntityName(),
							fdName,
							Integer.toString(weightedByColumnsAbsDep),				// correct (strict)
							Double.toString(columnsStrict.getRecall()),				// strict recall
							Double.toString(weightedByColumnsDep),					// correct (weighted)
							Double.toString(columnsWeighted.getRecall()),			// weighted recall
							Integer.toString(weightedByColumnsInClosureDep),		// correct (implied)
							Double.toString(columnsImplied.getRecall()),			// implied recall
							Integer.toString(totalColumnsDep),						// total original columns
							Integer.toString(valuesForDepInUniversal),				// total values for dependant in universal table
							depCol==null ? "unknown" : depCol.getDataType().toString()	// dependant data type
						}, "\t")));
					}
					fdIndex++;
				}
			}
			
			performanceLog.addPerformanceDetail(step + "FD Stitching (strict) - by class", ent.getEntityName(), new IRPerformance(1.0, corAbs / (double)ttl));
			performanceLog.addPerformanceDetail(step + "FD Stitching (weighted) - by class", ent.getEntityName(), new IRPerformance(1.0, cor / (double)ttl));
			performanceLog.addPerformanceDetail(step + "FD Stitching (in closure) - by class", ent.getEntityName(), new IRPerformance(1.0, inClosure / (double)ttl));
			performanceLog.addPerformanceDetail(step + "FD Stitching (matched columns strict) - by class", ent.getEntityName(), new IRPerformance(1.0, weightedByColumnsAbs / (double)totalColumns));
			performanceLog.addPerformanceDetail(step + "FD Stitching (matched columns weighted) - by class", ent.getEntityName(), new IRPerformance(1.0, weightedByColumns / (double)totalColumns));
			performanceLog.addPerformanceDetail(step + "FD Stitching (matched columns in closure) - by class", ent.getEntityName(), new IRPerformance(1.0, weightedByColumnsInClosure / (double)totalColumns));
			

			System.out.println(String.format("[EvaluateFunctionalDependencies] %.2f/%d FDs discovered correctly (%f%%), %d/%d in closure (%f%%)",
				cor,
				ttl,
				cor / (double)ttl * 100.0,
				inClosure,
				ttl,
				inClosure / (double)ttl * 100.0
			));
			logWriter.write(String.format("%.2f/%d FDs discovered correctly (%f%%), %d/%d in closure (%f%%)\n",
				cor,
				ttl,
				cor / (double)ttl * 100.0,
				inClosure,
				ttl,
				inClosure / (double)ttl * 100.0
			));
			resultsWriter.write(String.format("%s\n", StringUtils.join(new String[] {
				taskName,
				configuration,
				ent.getEntityName(),
				Integer.toString(corAbs),
				Double.toString(cor),
				Integer.toString(inClosure),
				Integer.toString(ttl),
				Integer.toString(weightedByColumnsAbs),
				Double.toString(weightedByColumns),
				Integer.toString(weightedByColumnsInClosure),
				Integer.toString(totalColumns),
				Double.toString(corAbs / (double)ttl),
				Double.toString(cor / (double)ttl),
				Double.toString(inClosure / (double)ttl),
				Double.toString(weightedByColumnsAbs / (double)totalColumns),
				Double.toString(weightedByColumns / (double)totalColumns),
				Double.toString(weightedByColumnsInClosure / (double)totalColumns)
			}, "\t")));

			ttlSum+=ttl;
			corSum+=cor;
			corAbsSum+=corAbs;
			inClosureSum+=inClosure;
			weightedByColumnsSum += weightedByColumns;
			weightedByColumnsAbsSum += weightedByColumnsAbs;
			weightedByColumnsInClosureSum += weightedByColumnsInClosure;
			totalColumnsSum += totalColumns;
		}

		performanceLog.addPerformance(step + "FD Stitching (strict) - by class", new Performance(corAbsSum, corAbsSum, ttlSum));
		performanceLog.addPerformance(step + "FD Stitching (weighted) - by class", new IRPerformance(1.0, corSum / (double)ttlSum));
		performanceLog.addPerformance(step + "FD Stitching (in closure) - by class", new Performance(inClosureSum, inClosureSum, ttlSum));
		performanceLog.addPerformance(step + "FD Stitching (matched columns strict) - by class", new Performance(weightedByColumnsAbsSum, weightedByColumnsAbsSum, totalColumnsSum)); 
		performanceLog.addPerformance(step + "FD Stitching (matched columns weighted) - by class", new IRPerformance(1.0, weightedByColumnsSum / (double)totalColumnsSum)); 
		performanceLog.addPerformance(step + "FD Stitching (matched columns in closure) - by class", new Performance(weightedByColumnsInClosureSum, weightedByColumnsInClosureSum, totalColumnsSum));

		performanceLog.addPerformance(step + "FD Stitching (strict)", new Performance(corAbsSum, corAbsSum, ttlSum));
		performanceLog.addPerformance(step + "FD Stitching (weighted)", new IRPerformance(1.0, corSum / (double)ttlSum));
		performanceLog.addPerformance(step + "FD Stitching (in closure)", new Performance(inClosureSum, inClosureSum, ttlSum));
		performanceLog.addPerformance(step + "FD Stitching (matched columns strict)", new Performance(weightedByColumnsAbsSum, weightedByColumnsAbsSum, totalColumnsSum)); 
		performanceLog.addPerformance(step + "FD Stitching (matched columns weighted)", new IRPerformance(1.0, weightedByColumnsSum / (double)totalColumnsSum)); 
		performanceLog.addPerformance(step + "FD Stitching (matched columns in closure)", new Performance(weightedByColumnsInClosureSum, weightedByColumnsInClosureSum, totalColumnsSum));

		resultsWriter.write(String.format("%s\n", StringUtils.join(new String[] {
			taskName,
			configuration,
			"all",
			Integer.toString(corAbsSum),
			Double.toString(corSum),
			Integer.toString(inClosureSum),
			Integer.toString(ttlSum),
			Integer.toString(weightedByColumnsAbsSum),
			Double.toString(weightedByColumnsSum),
			Integer.toString(weightedByColumnsInClosureSum),
			Integer.toString(totalColumnsSum),
			Double.toString(corAbsSum / (double)ttlSum),
			Double.toString(corSum / (double)ttlSum),
			Double.toString(inClosureSum / (double)ttlSum),
			Double.toString(weightedByColumnsAbsSum / (double)totalColumnsSum),
			Double.toString(weightedByColumnsSum / (double)totalColumnsSum),
			Double.toString(weightedByColumnsInClosureSum / (double)totalColumnsSum)
		}, "\t")));

		logWriter.close();
		resultsWriter.close();
		detailsWriter.close();

		return relationsForFDs;
	}

	public Map<EntityTable, Collection<Table>> extractRelationsForPredefinedFDs(
        StitchedModel universal, 
        Map<Table, EntityTable> entityTables, 
        Map<String, Table> entityIdToEntityTable,
        StitchedModel model,
		String taskName,
		Map<String, Integer> originalWebTableCount
    ) throws Exception {
		Map<EntityTable, Collection<Table>> relationsForFDs = new HashMap<>();

		// find the 'ne' table that contains all attribute definitions for non-key attributes
		// (only exists if entity structure was loaded from files, not available after matching)
		EntityTable nonEntityTable = null;
		for(EntityTable et : model.getEntityGroups()) {
			if(et.isNonEntity()) {
				nonEntityTable = et;
				break;
			}
		}

		for(Table t : universal.getTables().values()) {
			// get the corresponding entity table
			TableColumn foreignKey = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));
			Object firstFKValue = null;
			for(TableRow r : t.getRows()) {
				firstFKValue = r.get(foreignKey.getColumnIndex());
				if(firstFKValue!=null) {
					break;
				}
			}
			EntityTable ent = null;
			if(firstFKValue!=null) {
				Table et = entityIdToEntityTable.get(firstFKValue);
				ent = entityTables.get(et);
			}

			System.out.println(String.format("[extractRelationsForPredefinedFDs] Extracting relations for entity type '%s' from universal table {%s}", 
				ent.getEntityName(),
				StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ",")
			));

			List<Pair<Set<String>,Set<String>>> fdsToExtract = new LinkedList<>();
			for(Map.Entry<Set<String>,Set<String>> entry : ent.getFunctionalDependencies().entrySet()) {
				fdsToExtract.add(new Pair<>(entry.getKey(), entry.getValue()));
			}

			// sort FDs so we can use the created file name to match extracted relations and reference relations
			Collections.sort(fdsToExtract, (p1,p2)->
				StringUtils.join(Q.union(p1.getFirst(), p1.getSecond()), "")
				.compareTo(
					StringUtils.join(Q.union(p2.getFirst(), p2.getSecond()), "")
				)
			);

			Map<String, TableColumn> columnMapping = mapUniversalRelationsAttributesToReferenceEntityTables(t, ent, nonEntityTable, originalWebTableCount, false);
			Map<TableColumn, String> inverseMapping = MapUtils.invert(columnMapping);

			// assign numbers to each fd
			Map<Pair<Set<String>, String>, Integer> fdIds = new HashMap<>();
			int idx = 0;
			for(Pair<Set<String>, Set<String>> fdToCheck : fdsToExtract) {
				List<String> dependants = ent.sortByNumberOfOriginalColumns(fdToCheck.getSecond(), nonEntityTable, originalWebTableCount);
				for(String dependantAttribute : dependants) {
					fdIds.put(new Pair<>(fdToCheck.getFirst(), dependantAttribute), idx++);
				}
			}

			// extract the FDs
			for(Pair<Set<String>, Set<String>> fdToCheck : fdsToExtract) {
				System.out.println(String.format("[extractRelationsForPredefinedFDs] extracting {%s}->{%s}",
						StringUtils.join(fdToCheck.getFirst(), ","),
						StringUtils.join(fdToCheck.getSecond(), ",")
				));

				Set<String> determinantToCheck = new HashSet<>(fdToCheck.getFirst());

				Set<TableColumn> extraColumns = new HashSet<>();
				if(determinantToCheck.contains("FK")) {
					determinantToCheck.remove("FK");
					extraColumns.add(foreignKey);
				}
				Set<TableColumn> determinant = new HashSet<>(ent.translateAttributes(t, determinantToCheck, nonEntityTable));
				if(determinant.size()<determinantToCheck.size()) {
					System.out.println(String.format("[extractRelationsForPredefinedFDs]\tCould not translate determinant: {%s}->{%s} translated {%s}",
						StringUtils.join(fdToCheck.getFirst(), ","),
						StringUtils.join(fdToCheck.getSecond(), ","),
						StringUtils.join(Q.project(Q.union(determinant,extraColumns), (c)->c.getHeader()), ",")
					));
					continue;
				}
				determinant.addAll(extraColumns);

				List<String> dependants = ent.sortByNumberOfOriginalColumns(fdToCheck.getSecond(), nonEntityTable, originalWebTableCount);

				for(String dependantAttribute : dependants) {
					Set<TableColumn> dependant = new HashSet<>(ent.translateAttributes(t, Q.toSet(dependantAttribute), nonEntityTable));
					if(dependant.size()==0) {
						System.out.println(String.format("[extractRelationsForPredefinedFDs]\tCould not translate dependant: {%s}->{%s}",
							StringUtils.join(fdToCheck.getFirst(), ","),
							dependantAttribute
						));
						continue;
					}
					
					Collection<Table> relationsForEnt = MapUtils.get(relationsForFDs, ent, new LinkedList<>());
					for(TableColumn depCol : dependant) {
						Table fdRel = extractRelationForFD(new Pair<>(determinant, Q.toSet(depCol)), t, inverseMapping,  ent, nonEntityTable);

						//int fd_id = fdsToExtract.indexOf(fdToCheck);
						int fd_id = fdIds.get(new Pair<>(fdToCheck.getFirst(), dependantAttribute));
						//fdRel.setTableId(relationsForEnt.size());
						fdRel.setTableId(fd_id);
						fdRel.setPath(String.format("reference_%s_%s_fd_%d", ent.getEntityName(), taskName, fdRel.getTableId()));

						Table existing = Q.firstOrDefault(Q.where(relationsForEnt, (t0)->t0.getPath().equals(fdRel.getPath())));
						if(existing!=null) {
							if(fdRel.getSize()>existing.getSize()) {
								System.out.println(String.format("[extractRelationsForPredefinedFDs] replacing %s (was: %,d rows; now: %,d rows)", existing.getPath(), existing.getSize(), fdRel.getSize()));
								relationsForEnt.remove(existing);
							} else {
								continue;
							}
						}
						System.out.println(String.format("[extractRelationsForPredefinedFDs] extracted {%s}->{%s} as %s",
							StringUtils.join(Q.project(determinant, (c)->c.getHeader()), ","),
							depCol.getHeader(),
							fdRel.getPath()
						));
						relationsForEnt.add(fdRel);
					}
				}
			}

		}

		return relationsForFDs;
	}

	/**
	 * Extract a relation for a given FD, expects the dependant to be a single attribute
	 * @throws Exception
	 */
	public static Table extractRelationForFD(
		Pair<Set<TableColumn>,Set<TableColumn>> fd, 
		Table t, 
		Map<TableColumn, String> colToAttribute,
		EntityTable et, 
		EntityTable nonEntityTable
	) throws Exception {

		Collection<TableColumn> fdColumns = Q.union(fd.getFirst(), fd.getSecond());
		Table fdRel = t.project(fdColumns);
		Map<Integer, Integer> indexProjection = t.projectColumnIndices(fdColumns);
		fdRel.deduplicate(fdRel.getColumns(), ConflictHandling.KeepBoth);

		// set the extracted FD
		fdRel.getSchema().getFunctionalDependencies().clear();
		Set<TableColumn> projectedDeterminant = new HashSet<>(Q.project(fd.getFirst(), (c)->fdRel.getSchema().get(indexProjection.get(c.getColumnIndex()))));
		TableColumn depCol = Q.firstOrDefault(fd.getSecond());
		TableColumn projectedDependant = fdRel.getSchema().get(indexProjection.get(depCol.getColumnIndex()));
		fdRel.getSchema().getFunctionalDependencies().put(projectedDeterminant, Q.toSet(projectedDependant));

		for(TableColumn c : fdColumns) {
			String attributeName = colToAttribute.get(c);
			if(attributeName!=null) {
				fdRel.getSchema().get(indexProjection.get(c.getColumnIndex())).setHeader(attributeName);
			}
		}
		// et.renameAttributes(fdRel, attributeNames, nonEntityTable);

		// remove rows with empty dependant
		ArrayList<TableRow> rows = new ArrayList<>();
		for(TableRow r : fdRel.getRows()) {
			if(r.get(projectedDependant.getColumnIndex())!=null) {
				rows.add(r);
			}
		}
		fdRel.setRows(rows);
		fdRel.reorganiseRowNumbers();

		// remove duplicates
		fdRel.deduplicate(fdRel.getColumns(), ConflictHandling.KeepFirst);

		return fdRel;
	}

    public static void setPredefinedFunctionalDependencies(StitchedModel universal, Map<Table, EntityTable> entityTables, Map<String, Table> entityIdToEntityTable, StitchedModel model) {
		// find the 'ne' table that contains all attribute definitions for non-key attributes
		// (only exists if entity structure was loaded from files, not available after matching)
		EntityTable nonEntityTable = null;
		for(EntityTable et : model.getEntityGroups()) {
			if(et.isNonEntity()) {
				nonEntityTable = et;
				break;
			}
		}

		for(Table t : universal.getTables().values()) {
			// get the corresponding entity table
			TableColumn foreignKey = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));
			Object firstFKValue = null;
			for(TableRow r : t.getRows()) {
				firstFKValue = r.get(foreignKey.getColumnIndex());
				if(firstFKValue!=null) {
					break;
				}
			}
			EntityTable ent = null;
			if(firstFKValue!=null) {
				Table et = entityIdToEntityTable.get(firstFKValue);
				ent = entityTables.get(et);
			}

			System.out.println(String.format("[setPredefinedFunctionalDependencies] Setting pre-defined FDs for entity type '%s' in universal table #%d {%s}",
				ent.getEntityName(),
				t.getTableId(),
				StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ",")
			));

			// set the FDs
			for(Pair<Set<String>, Set<String>> fdToSet : Pair.fromMap(ent.getFunctionalDependencies())) {
				Set<TableColumn> extraColumns = new HashSet<>();
				if(fdToSet.getFirst().contains("FK")) {
					fdToSet = new Pair<>(new HashSet<>(Q.without(fdToSet.getFirst(), Q.toSet("FK"))), fdToSet.getSecond());
					extraColumns.add(foreignKey);
				}
				Set<TableColumn> determinant = new HashSet<>(ent.translateAttributes(t, fdToSet.getFirst(), nonEntityTable));
				if(determinant.size()<fdToSet.getFirst().size()) {
					System.out.println(String.format("[setPredefinedFunctionalDependencies] Could not translate determinant: {%s}->{%s} translated {%s}",
						StringUtils.join(fdToSet.getFirst(), ","),
						StringUtils.join(fdToSet.getSecond(), ","),
						StringUtils.join(Q.project(determinant, (c)->c.getHeader()), ",")
					));
				}
				determinant.addAll(extraColumns);
				Set<TableColumn> dependant = new HashSet<>(ent.translateAttributes(t, fdToSet.getSecond(), nonEntityTable));
				if(determinant.size()<fdToSet.getFirst().size()) {
					System.out.println(String.format("[setPredefinedFunctionalDependencies] Could not translate dependant: {%s}->{%s} translated {%s}",
						StringUtils.join(fdToSet.getFirst(), ","),
						StringUtils.join(fdToSet.getSecond(), ","),
						StringUtils.join(Q.project(dependant, (c)->c.getHeader()), ",")
					));
				}
				t.getSchema().getFunctionalDependencies().put(determinant, dependant);
			}
		}
	}

}
