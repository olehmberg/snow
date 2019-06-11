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
package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.winter.matching.blockers.AbstractBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.Blocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer.VectorCreationMethod;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.LeftIdentityPair;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.processing.DataAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollector;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.CountAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SetAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SumDoubleAggregator;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 * @param <RecordType>			the type of records which are the input for the blocking operation
 * @param <SchemaElementType>	the type of schema elements that are used in the schema of RecordType 
 * @param <BlockedType>			the type of record which is actually blocked
 * @param <CorrespondenceType>	the type of correspondences which are the input for the blocking operation
 */
public class ValueBasedCorrespondenceRescorer<RecordType extends Matchable, SchemaElementType extends Matchable, BlockedType extends Matchable, CorrespondenceType extends Matchable>
{

	protected class BlockingVector extends HashMap<String, Double> {
		private static final long serialVersionUID = 1L;
		
		private Processable<Correspondence<CorrespondenceType, Matchable>> correspondences = new ProcessableCollection<>();
		/**
		 * @return the correspondences
		 */
		public Processable<Correspondence<CorrespondenceType, Matchable>> getCorrespondences() {
			return correspondences;
		}
		/**
		 * @param correspondences the correspondences to set
		 */
		public void setCorrespondences(Processable<Correspondence<CorrespondenceType, Matchable>> correspondences) {
			this.correspondences = correspondences;
		}
		public void addCorrespondences(Processable<Correspondence<CorrespondenceType, Matchable>> correspondences) {
			this.correspondences = this.correspondences.append(correspondences);
		}
	}
	protected class Block extends LeftIdentityPair<String, Set<BlockedType>> {
		private static final long serialVersionUID = 1L;

		public Block(String first, Set<BlockedType> second) {
			super(first, second);
		}
	}
	protected class BlockJoinKeyGenerator implements Function<String,Block> {
		private static final long serialVersionUID = 1L;

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.winter.processing.Function#execute(java.lang.Object)
		 */
		@Override
		public String execute(Block input) {
			return input.getFirst();
		}
		
	}
	
	private BlockingKeyGenerator<RecordType, CorrespondenceType, BlockedType> blockingFunction;
	private BlockingKeyGenerator<RecordType, CorrespondenceType, BlockedType> secondBlockingFunction;
	private VectorSpaceSimilarity similarityFunction;
	private boolean measureBlockSizes = false;


	private VectorCreationMethod vectorCreationMethod;
	private double similarityThreshold;
	
	/**
	 * @param measureBlockSizes the measureBlockSizes to set
	 */
	public void setMeasureBlockSizes(boolean measureBlockSizes) {
		this.measureBlockSizes = measureBlockSizes;
	}
	
	/**
	 * @return the similarityFunction
	 */
	public VectorSpaceSimilarity getSimilarityFunction() {
		return similarityFunction;
	}
	
	public ValueBasedCorrespondenceRescorer(
        BlockingKeyGenerator<RecordType, CorrespondenceType, BlockedType> blockingFunction, 
        BlockingKeyGenerator<RecordType, CorrespondenceType, BlockedType> secondBlockingFunction, 
        VectorSpaceSimilarity similarityFunction, 
        VectorCreationMethod vectorCreationMethod, 
        double similarityThreshold
    ) {
		this.blockingFunction = blockingFunction;
		this.secondBlockingFunction = secondBlockingFunction == null ? blockingFunction : secondBlockingFunction;
		this.similarityFunction = similarityFunction;
		this.vectorCreationMethod = vectorCreationMethod;
		this.similarityThreshold = similarityThreshold;
	}
	
	public Processable<Correspondence<BlockedType, Matchable>> run(
			DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2,
			Processable<Correspondence<BlockedType, Matchable>> correspondences) {

		// create blocking key value vectors
		System.out.println("[BlockingKeyIndexer] Creating blocking key value vectors");
        Processable<Pair<BlockedType, BlockingVector>> vectors1 = createBlockingVectors(dataset1, blockingFunction);
        Processable<Pair<BlockedType, BlockingVector>> vectors2 = null;
        if(dataset1==dataset2) {
            vectors2 = vectors1;
        } else {
            vectors2 = createBlockingVectors(dataset2, secondBlockingFunction);
        }
		
		
		// create inverted index
		System.out.println("[BlockingKeyIndexer] Creating inverted index");
		Processable<Block> blocks1 = createInvertedIndex(vectors1);
		Processable<Block> blocks2 = createInvertedIndex(vectors2);
		
		if(vectorCreationMethod==VectorCreationMethod.TFIDF) {
			System.out.println("[BlockingKeyIndexer] Calculating TFIDF vectors");
			// update blocking key value vectors to TF-IDF weights
			Processable<Pair<String, Double>> documentFrequencies = createDocumentFrequencies(blocks1, blocks2);
			int documentCount = vectors1.size() + vectors2.size();
			vectors1 = createTFIDFVectors(vectors1, documentFrequencies, documentCount);
			vectors2 = createTFIDFVectors(vectors2, documentFrequencies, documentCount);
		}
        
        // join the provided correspondences with vectors on BlockedType
        System.out.println("[BlockingKeyIndexer] Joining correspondences with vectors");
		Processable<Correspondence<BlockedType, Matchable>> rescoredCorrespondences = correspondences
			.join(vectors1, (c)->c.getFirstRecord(), (p)->p.getFirst())
			.join(vectors2, (p)->p.getFirst().getSecondRecord(), (p)->p.getFirst())
			.map((Pair<Pair<Correspondence<BlockedType, Matchable>, Pair<BlockedType, BlockingVector>>, Pair<BlockedType, BlockingVector>> record, DataIterator<Correspondence<BlockedType, Matchable>> resultCollector) 
				-> {

                    Correspondence<BlockedType, Matchable> cor = record.getFirst().getFirst();
                    BlockingVector leftVector = record.getFirst().getSecond().getSecond();
                    BlockingVector rightVector = record.getSecond().getSecond();

                    // calculate the new similarity score
                    double newScore = 0;
                    for(String dimension : leftVector.keySet()) {
                        if(rightVector.containsKey(dimension)) {
                            newScore = similarityFunction.aggregateDimensionScores(newScore, similarityFunction.calculateDimensionScore(leftVector.get(dimension), rightVector.get(dimension)));
                        }
                    }
                    newScore = similarityFunction.normaliseScore(newScore, leftVector, rightVector);

                    if(newScore>=similarityThreshold) {
                        resultCollector.next(new Correspondence<>(cor.getFirstRecord(), cor.getSecondRecord(), newScore, cor.getCausalCorrespondences()));
                    }
				});

        return rescoredCorrespondences;
	}
	
	protected void measureBlockSizes(Processable<Triple<String, BlockedType, BlockedType>> pairs) {
		// calculate block size distribution
		Processable<Pair<String, Integer>> aggregated = pairs.aggregate(
			(Triple<String, BlockedType, BlockedType> record,
			DataIterator<Pair<String, Integer>> resultCollector) 
			-> {
				resultCollector.next(new Pair<String, Integer>(record.getFirst(), 1));
			}
			, new CountAggregator<>());

			System.out.println("50 most-frequent blocking key values:");
			for(Pair<String, Integer> value : aggregated.sort((v)->v.getSecond(), false).take(50).get()) {
				System.out.println(String.format("\t%d\t%s", value.getSecond(), value.getFirst()));
			}
	}
	
	protected 
	Processable<Pair<BlockedType, BlockingVector>> 
	createBlockingVectors(
			Processable<RecordType> ds, 
			BlockingKeyGenerator<RecordType, CorrespondenceType, BlockedType> blockingFunction) {
		
		// input: a dataset of records
		return ds
				.aggregate(
						new RecordKeyValueMapper<BlockedType, RecordType, String>() {
		
					private static final long serialVersionUID = 1L;
		
					@Override
					public void mapRecordToKey(
							RecordType record,
							DataIterator<Pair<BlockedType, String>> resultCollector) {

						// apply the blocking key generator to the current record
						Processable<RecordType> col = new ProcessableCollection<>();
						col.add(record);
                        Processable<Pair<String, BlockedType>> blockingKeyValues = col.map((r,results)->{
                            ProcessableCollector<Pair<String, BlockedType>> collector = new ProcessableCollector<>();
                            collector.setResult(new ProcessableCollection<>());
                            collector.initialise();
                            
                            // execute the blocking funtion
                            blockingFunction.generateBlockingKeys(record, null, collector);
                            
                            collector.finalise();
                            
                            for(Pair<String, BlockedType> p : collector.getResult().get()) {
                                results.next(p);
                            }
                        });
						
						// then create pairs of (blocking key value, correspondences) and group them by the blocked element
						for(Pair<String, BlockedType> p : blockingKeyValues.get()) {
							BlockedType blocked = p.getSecond();
							String blockingKeyValue = p.getFirst();
							resultCollector.next(new Pair<>(blocked, blockingKeyValue));
						}
					}
				},
						// aggregate the blocking key values for each blocked element into blocking vectors
				new DataAggregator<BlockedType, String, BlockingVector>() {
		
					private static final long serialVersionUID = 1L;
		
					@Override
					public Pair<BlockingVector,Object> initialise(
							BlockedType keyValue) {
						return stateless(new BlockingVector());
					}
		
					@Override
					public Pair<BlockingVector,Object> aggregate(
							BlockingVector previousResult,
							String record,
							Object state) {
						
						// get the dimension for the current blocking key value in the blocking vector
						Double existing = previousResult.get(record);
						
						if(existing==null) {
							existing = 0.0;
							
						}
						
						// increment the frequency for this blocking key value
						Double frequency = existing+1;

						existing = frequency;
						
						previousResult.put(record, existing);
						return stateless(previousResult);
					}
					
					/* (non-Javadoc)
					 * @see de.uni_mannheim.informatik.dws.winter.processing.DataAggregator#merge(de.uni_mannheim.informatik.dws.winter.model.Pair, de.uni_mannheim.informatik.dws.winter.model.Pair)
					 */
					@Override
					public Pair<BlockingVector, Object> merge(
							Pair<BlockingVector, Object> intermediateResult1,
							Pair<BlockingVector, Object> intermediateResult2) {
						
						BlockingVector first = intermediateResult1.getFirst();
						BlockingVector second = intermediateResult2.getFirst();
						
						Set<String> keys = Q.union(first.keySet(), second.keySet());
						
						BlockingVector result = new BlockingVector();
						result.addCorrespondences(first.getCorrespondences());
						result.addCorrespondences(second.getCorrespondences());
						
						for(String k : keys) {
							Double v1 = first.get(k);
							Double v2 = second.get(k);
							
							if(v1==null) {
								v1 = v2;
							} else if(v2!=null) {
								v1 = v1 + v2;
							}
							
							result.put(k, v1);
						}
						
						return stateless(result);
					}
					
					/* (non-Javadoc)
					 * @see de.uni_mannheim.informatik.dws.winter.processing.DataAggregator#createFinalValue(java.lang.Object, java.lang.Object)
					 */
					@Override
					public BlockingVector createFinalValue(
							BlockedType keyValue,
							BlockingVector result,
							Object state) {
						
						BlockingVector vector = new BlockingVector();
						vector.addCorrespondences(result.getCorrespondences());
						
						for(String s : result.keySet()) {
							Double d = result.get(s);
							
							if(vectorCreationMethod==VectorCreationMethod.BinaryTermOccurrences) {
								d = Math.min(1.0, d);
							} else {
								d = d / result.size();
							}
							
							vector.put(s, d);
						}
						
						return vector;
					}
				});
	}

	protected Processable<Block> createInvertedIndex(Processable<Pair<BlockedType, BlockingVector>> vectors) {
		
		return vectors
				.aggregate((Pair<BlockedType, BlockingVector> record, DataIterator<Pair<String, BlockedType>> resultCollector) 
				-> {
					
					for(String s : record.getSecond().keySet()) {
						resultCollector.next(new Pair<>(s, record.getFirst()));
					}
					
				}, 
				new SetAggregator<>())
				.map((Pair<String, Set<BlockedType>> record,DataIterator<Block> resultCollector) 
				-> {
						
						resultCollector.next(new Block(record.getFirst(), record.getSecond()));;
						
				});
		
	}
	
	protected Processable<Pair<String, Double>> createDocumentFrequencies(Processable<Block> blocks1, Processable<Block> blocks2) {
		
		// calculate document frequencies
		Processable<Pair<String, Double>> df1 = blocks1
				.map((Block record,DataIterator<Pair<String, Double>> resultCollector) 
					-> {
						resultCollector.next(new Pair<>(record.getFirst(), (double)record.getSecond().size()));
					});
		
		Processable<Pair<String, Double>> df2 = blocks2
				.map((Block record,DataIterator<Pair<String, Double>> resultCollector) 
					-> {
						resultCollector.next(new Pair<>(record.getFirst(), (double)record.getSecond().size()));
					});
		
		return df1
			.append(df2)
			.aggregate((Pair<String, Double> record, DataIterator<Pair<String, Double>> resultCollector) 
				-> {
					resultCollector.next(record);
				}
				, new SumDoubleAggregator<>());
	}
	
	protected Processable<Pair<BlockedType, BlockingVector>> createTFIDFVectors(Processable<Pair<BlockedType, BlockingVector>> vectors, Processable<Pair<String, Double>> documentFrequencies, int documentCount) {
	
		Map<String, Double> dfMap = Q.map(documentFrequencies.get(), (p)->p.getFirst(), (p)->p.getSecond());
		
		return vectors
				.map((Pair<BlockedType, BlockingVector> record, DataIterator<Pair<BlockedType, BlockingVector>> resultCollector) 
					-> {
						BlockingVector tfVector = record.getSecond();
						BlockingVector tfIdfVector = new BlockingVector();
						
						for(String s : tfVector.keySet()) {
							Double tfScore = tfVector.get(s);;
							
							double df = dfMap.get(s);
							double tfIdfScore = tfScore * Math.log( documentCount / df );
							
							tfIdfVector.put(s, tfIdfScore);
						}
						
						resultCollector.next(new Pair<>(record.getFirst(), tfIdfVector));
					});
		
	}
	
	protected Processable<Correspondence<BlockedType, CorrespondenceType>> createCorrespondences(Processable<Triple<String, Pair<BlockedType, BlockingVector>, Pair<BlockedType, BlockingVector>>> pairsWithVectors) {
		return pairsWithVectors
				.aggregate(
					(Triple<String, Pair<BlockedType, BlockingVector>, Pair<BlockedType, BlockingVector>> record, DataIterator<Pair<Pair<Pair<BlockedType,BlockingVector>, Pair<BlockedType,BlockingVector>>, Pair<Double,Double>>> resultCollector) 
					-> {
						String dimension = record.getFirst();
						
						BlockedType leftRecord = record.getSecond().getFirst();
						BlockedType rightRecord = record.getThird().getFirst();
						
						BlockingVector leftVector = record.getSecond().getSecond();
						BlockingVector rightVector = record.getThird().getSecond();
						
						Pair<Pair<BlockedType, BlockingVector>,Pair<BlockedType, BlockingVector>> key = new Pair<>(new LeftIdentityPair<>(leftRecord,leftVector), new LeftIdentityPair<>(rightRecord,rightVector));
						Pair<Double,Double> value = new Pair<>(leftVector.get(dimension), rightVector.get(dimension));
						
						resultCollector.next(new Pair<>(key, value));
					}, 
					new DataAggregator<
						Pair<Pair<BlockedType, BlockingVector>, Pair<BlockedType, BlockingVector>>, 
						Pair<Double, Double>,
						Correspondence<BlockedType, CorrespondenceType>
						>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Pair<Correspondence<BlockedType, CorrespondenceType>, Object> initialise(
							Pair<Pair<BlockedType, BlockingVector>, Pair<BlockedType, BlockingVector>> keyValue) {
						return stateless(new Correspondence<>(keyValue.getFirst().getFirst(), keyValue.getSecond().getFirst(), 0.0));
					}

					@Override
					public Pair<Correspondence<BlockedType, CorrespondenceType>, Object> aggregate(
							Correspondence<BlockedType, CorrespondenceType> previousResult,
							Pair<Double,Double> record,
							Object state) {

						Double leftEntry = record.getFirst();
						Double rightEntry = record.getSecond();
						
						double score = similarityFunction.calculateDimensionScore(leftEntry, rightEntry);

						score = similarityFunction.aggregateDimensionScores(previousResult.getSimilarityScore(), score);					
						
						return stateless(new Correspondence<BlockedType, CorrespondenceType>(previousResult.getFirstRecord(), previousResult.getSecondRecord(), score, null));
					}
					
					@Override
					public Pair<Correspondence<BlockedType, CorrespondenceType>, Object> merge(
							Pair<Correspondence<BlockedType, CorrespondenceType>, Object> intermediateResult1,
							Pair<Correspondence<BlockedType, CorrespondenceType>, Object> intermediateResult2) {

						Correspondence<BlockedType, CorrespondenceType> c1 = intermediateResult1.getFirst();
						Correspondence<BlockedType, CorrespondenceType> c2 = intermediateResult2.getFirst();
						
						Correspondence<BlockedType, CorrespondenceType> result = new Correspondence<>(
								c1.getFirstRecord(), 
								c1.getSecondRecord(), 
								similarityFunction.aggregateDimensionScores(c1.getSimilarityScore(), c2.getSimilarityScore()));
						
						return stateless(result);
					}
					
					public Correspondence<BlockedType,CorrespondenceType> createFinalValue(Pair<Pair<BlockedType, BlockingVector>,Pair<BlockedType, BlockingVector>> keyValue, Correspondence<BlockedType,CorrespondenceType> result, Object state) {

						BlockedType record1 = keyValue.getFirst().getFirst();
						BlockedType record2 = keyValue.getSecond().getFirst();
						
						BlockingVector leftVector = keyValue.getFirst().getSecond();
						BlockingVector rightVector = keyValue.getSecond().getSecond();
						
						double similarityScore = similarityFunction.normaliseScore(result.getSimilarityScore(), leftVector, rightVector);
						
						if(similarityScore>=similarityThreshold) {						
							Processable<Correspondence<CorrespondenceType, Matchable>> causes = createCausalCorrespondences(record1, record2, leftVector, rightVector);
							
							return new Correspondence<>(result.getFirstRecord(), result.getSecondRecord(), similarityScore, causes);
						} else {
							return null;
						}
					}
				})
				.map((Pair<Pair<Pair<BlockedType, BlockingVector>, Pair<BlockedType, BlockingVector>>, Correspondence<BlockedType, CorrespondenceType>> record, DataIterator<Correspondence<BlockedType, CorrespondenceType>> resultCollector) 
					-> {
						resultCollector.next(record.getSecond());
					});
	}
	
	protected Processable<Correspondence<CorrespondenceType, Matchable>> createCausalCorrespondences(
			BlockedType record1, 
			BlockedType record2,
			BlockingVector vector1,
			BlockingVector vector2) {
		
		Processable<Correspondence<CorrespondenceType, Matchable>> causes = 
				new ProcessableCollection<>(vector1.getCorrespondences().get())
				.append(vector2.getCorrespondences())
				.distinct();
		
		int[] pairIds = new int[] { record1.getDataSourceIdentifier(), record2.getDataSourceIdentifier() };
		Arrays.sort(pairIds);
		
		// filter the correspondences such that only correspondences between the two records are contained (by data source id)
		return causes.where((c)-> {
		
			int[] causeIds = new int[] { c.getFirstRecord().getDataSourceIdentifier(), c.getSecondRecord().getDataSourceIdentifier() };
			Arrays.sort(causeIds);
			
			return Arrays.equals(pairIds, causeIds);
		});
	}
}
