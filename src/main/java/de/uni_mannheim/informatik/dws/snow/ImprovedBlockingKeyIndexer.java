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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

import de.uni_mannheim.informatik.dws.winter.matching.blockers.AbstractBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.Blocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.LeftIdentityPair;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.processing.DataAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.CountAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SetAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SumDoubleAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 * @param <RecordType>
 *            the type of records which are the input for the blocking operation
 * @param <SchemaElementType>
 *            the type of schema elements that are used in the schema of
 *            RecordType
 * @param <BlockedType>
 *            the type of record which is actually blocked
 * @param <CorrespondenceType>
 *            the type of correspondences which are the input for the blocking
 *            operation
 */
public class ImprovedBlockingKeyIndexer<RecordType extends Matchable, SchemaElementType extends Matchable, BlockedType extends Matchable, CorrespondenceType extends Matchable>
		extends AbstractBlocker<RecordType, BlockedType, CorrespondenceType>
		implements Blocker<RecordType, SchemaElementType, BlockedType, CorrespondenceType> // ,
// SymmetricBlocker<RecordType, SchemaElementType, BlockedType,
// CorrespondenceType>
{

	private static final Logger logger = WinterLogManager.getLogger();

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
		 * @param correspondences
		 *            the correspondences to set
		 */
		public void setCorrespondences(Processable<Correspondence<CorrespondenceType, Matchable>> correspondences) {
			this.correspondences = correspondences;
		}

		public void addCorrespondences(Processable<Correspondence<CorrespondenceType, Matchable>> correspondences) {
			this.correspondences = this.correspondences.append(correspondences);
		}
	}

	protected class Block extends LeftIdentityPair<String, Set<Pair<BlockedType, BlockingVector>>> {
		private static final long serialVersionUID = 1L;

		public Block(String first, Set<Pair<BlockedType, BlockingVector>> second) {
			super(first, second);
		}
	}

	protected class BlockJoinKeyGenerator implements Function<String, Block> {
		private static final long serialVersionUID = 1L;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * de.uni_mannheim.informatik.dws.winter.processing.Function#execute(
		 * java.lang.Object)
		 */
		@Override
		public String execute(Block input) {
			return input.getFirst();
		}

	}

	private BlockingKeyGenerator<RecordType, CorrespondenceType, BlockedType> blockingFunction;
	private BlockingKeyGenerator<RecordType, CorrespondenceType, BlockedType> secondBlockingFunction;
    private VectorSpaceSimilarity similarityFunction;
    private double blockFilterRatio = 1.0;
	private int maxBlockPairSize = 0;
    private boolean recalculateScores = false;
    private boolean cacheBlocks = false;
    private Processable<Block> blocks1;
    private Processable<Block> blocks2;

	public enum VectorCreationMethod {
		BinaryTermOccurrences, TermFrequencies, TFIDF
	}

	public enum DocumentFrequencyCounter {
		Dataset1, Dataset2, Both, Preset
	}

	private VectorCreationMethod vectorCreationMethod;
	private double similarityThreshold;
	private DocumentFrequencyCounter documentFrequencyCounter = DocumentFrequencyCounter.Both;
	private Processable<Pair<String, Double>> inverseDocumentFrequencies;

	/**
	 * @return the similarityFunction
	 */
	public VectorSpaceSimilarity getSimilarityFunction() {
		return similarityFunction;
	}

	/**
	 * @param documentFrequencyCounter the documentFrequencyCounter to set
	 */
	public void setDocumentFrequencyCounter(DocumentFrequencyCounter documentFrequencyCounter) {
		this.documentFrequencyCounter = documentFrequencyCounter;
	}

	/**
	 * @return the inverseDocumentFrequencies
	 */
	public Processable<Pair<String, Double>> getInverseDocumentFrequencies() {
		return inverseDocumentFrequencies;
	}
	/**
	 * @param inverseDocumentFrequencies the inverseDocumentFrequencies to set
	 */
	public void setInverseDocumentFrequencies(Processable<Pair<String, Double>> inverseDocumentFrequencies) {
		this.inverseDocumentFrequencies = inverseDocumentFrequencies;
	}

    /**
     * @param blockFilterRatio the blockFilterRatio to set
     */
    public void setBlockFilterRatio(double blockFilterRatio) {
        this.blockFilterRatio = blockFilterRatio;
    }

    /**
     * @param maxBlockPairSize the maxBlockPairSize to set
     */
    public void setMaxBlockPairSize(int maxBlockPairSize) {
        this.maxBlockPairSize = maxBlockPairSize;
    }

    /**
     * Determines whether the similarity scores are re-calculated after indexing.
     * 
     * @param recalculateScores if false, scores are determined from overlapping tokens in the index, i.e., tokens removed during block filtering are ignored
     *                          if true, scores are recalculated based on all tokens of the respective records
     */
    public void setRecalculateScores(boolean recalculateScores) {
        this.recalculateScores = recalculateScores;
    }

    /**
     * @param cacheBlocks the cacheBlocks to set
     */
    public void setCacheBlocks(boolean cacheBlocks) {
        this.cacheBlocks = cacheBlocks;
    }

    public void resetCachedBlocks(boolean dataset1, boolean dataset2) {
        if(dataset1) {
            blocks1 = null;
        }
        if(dataset2) {
            blocks2 = null;
        }
    }

	// public BlockingKeyIndexer(BlockingKeyGenerator<RecordType,
	// CorrespondenceType, BlockedType> blockingFunction, VectorSpaceSimilarity
	// similarityFunction) {
	// this.blockingFunction = blockingFunction;
	// this.secondBlockingFunction = blockingFunction;
	// this.similarityFunction = similarityFunction;
	// }

	public ImprovedBlockingKeyIndexer(BlockingKeyGenerator<RecordType, CorrespondenceType, BlockedType> blockingFunction,
			BlockingKeyGenerator<RecordType, CorrespondenceType, BlockedType> secondBlockingFunction,
			VectorSpaceSimilarity similarityFunction, VectorCreationMethod vectorCreationMethod,
			double similarityThreshold) {
		this.blockingFunction = blockingFunction;
		this.secondBlockingFunction = secondBlockingFunction == null ? blockingFunction : secondBlockingFunction;
		this.similarityFunction = similarityFunction;
		this.vectorCreationMethod = vectorCreationMethod;
		this.similarityThreshold = similarityThreshold;
	}

	public Processable<Pair<String, Double>> calculateInverseDocumentFrequencies(
		DataSet<RecordType, SchemaElementType> dataset, 
		BlockingKeyGenerator<RecordType, CorrespondenceType, BlockedType> blockingFunction) {

		DocumentFrequencyCounter documentFrequencyCounter = DocumentFrequencyCounter.Dataset1;

		Processable<Pair<RecordType, Processable<Correspondence<CorrespondenceType, Matchable>>>> ds = combineDataWithCorrespondences(
			dataset, null,
			(r, c) -> c.next(new Pair<>(r.getFirstRecord().getDataSourceIdentifier(), r)));

		logger.info("Creating blocking key value vectors");
		Processable<Pair<BlockedType, BlockingVector>> vectors1 = createBlockingVectors(ds, blockingFunction);

		logger.info("Creating inverted index");
		Processable<Block> blocks1 = createInvertedIndex(vectors1);

		logger.info("Calculating TFIDF vectors");
		// update blocking key value vectors to TF-IDF weights

		Processable<Pair<String, Double>> documentFrequencies = createDocumentFrequencies(blocks1, new ParallelProcessableCollection<Block>(), documentFrequencyCounter);
		int documentCount = getDocumentCount(vectors1, new ParallelProcessableCollection<Pair<BlockedType, BlockingVector>>(), documentFrequencyCounter);
		Processable<Pair<String, Double>> inverseDocumentFrequencies = createIDF(documentFrequencies, documentCount);

		return inverseDocumentFrequencies;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.uni_mannheim.informatik.dws.winter.matching.blockers.Blocker#
	 * runBlocking(de.uni_mannheim.informatik.dws.winter.model.DataSet,
	 * de.uni_mannheim.informatik.dws.winter.model.DataSet,
	 * de.uni_mannheim.informatik.dws.winter.processing.Processable)
	 */
	@Override
	public Processable<Correspondence<BlockedType, CorrespondenceType>> runBlocking(
			DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2,
			Processable<Correspondence<CorrespondenceType, Matchable>> schemaCorrespondences) {

        Processable<Pair<BlockedType, BlockingVector>> vectors1 = null;
        Processable<Pair<BlockedType, BlockingVector>> vectors2 = null;

        if(!cacheBlocks || blocks1 == null) {
            logger.info("Indexing dataset1");
            // combine the datasets with the schema correspondences
            Processable<Pair<RecordType, Processable<Correspondence<CorrespondenceType, Matchable>>>> ds1 = combineDataWithCorrespondences(
                    dataset1, schemaCorrespondences,
                    (r, c) -> c.next(new Pair<>(r.getFirstRecord().getDataSourceIdentifier(), r)));
            
            // create blocking key value vectors
            vectors1 = createBlockingVectors(ds1, blockingFunction);
            // create inverted index
            blocks1 = createInvertedIndex(vectors1);
        }

        if(!cacheBlocks || blocks2 == null) {
            logger.info("Indexing dataset2");
            Processable<Pair<RecordType, Processable<Correspondence<CorrespondenceType, Matchable>>>> ds2 = combineDataWithCorrespondences(
                    dataset2, schemaCorrespondences,
                    (r, c) -> c.next(new Pair<>(r.getSecondRecord().getDataSourceIdentifier(), r)));
            vectors2 = createBlockingVectors(ds2, secondBlockingFunction);
            blocks2 = createInvertedIndex(vectors2);
        }

		if (vectorCreationMethod == VectorCreationMethod.TFIDF) {
			logger.info("Calculating TFIDF vectors");
			// update blocking key value vectors to TF-IDF weights

			if(documentFrequencyCounter!=DocumentFrequencyCounter.Preset || inverseDocumentFrequencies==null) {
                Processable<Pair<String, Double>> documentFrequencies = createDocumentFrequencies(blocks1, blocks2, documentFrequencyCounter);

                if(vectors1==null) {
                    // combine the datasets with the schema correspondences
                    Processable<Pair<RecordType, Processable<Correspondence<CorrespondenceType, Matchable>>>> ds1 = combineDataWithCorrespondences(
                        dataset1, schemaCorrespondences,
                        (r, c) -> c.next(new Pair<>(r.getFirstRecord().getDataSourceIdentifier(), r)));

                    // create blocking key value vectors
                    vectors1 = createBlockingVectors(ds1, blockingFunction);
                }
                if(vectors2==null) {
                    Processable<Pair<RecordType, Processable<Correspondence<CorrespondenceType, Matchable>>>> ds2 = combineDataWithCorrespondences(
                        dataset2, schemaCorrespondences,
                        (r, c) -> c.next(new Pair<>(r.getSecondRecord().getDataSourceIdentifier(), r)));
                    vectors2 = createBlockingVectors(ds2, secondBlockingFunction);
                }

				int documentCount = getDocumentCount(vectors1, vectors2, documentFrequencyCounter);
				inverseDocumentFrequencies = createIDF(documentFrequencies, documentCount);
			}

            if(vectors1!=null) {
                vectors1 = createTFIDFVectors(vectors1, inverseDocumentFrequencies);
                blocks1 = createInvertedIndex(vectors1);
            }
            if(vectors2!=null) {
                vectors2 = createTFIDFVectors(vectors2, inverseDocumentFrequencies);
                blocks2 = createInvertedIndex(vectors2);
            }
            
		}

        if(isMeasureBlockSizes()) {
            measureBlockSizes(blocks1, blocks2);
        }

        logger.info("Block Filtering");
        long maxBlockSize = maxBlockPairSize==0 ? Long.MAX_VALUE : maxBlockPairSize;
        Processable<Long> blockSizes = blocks1.join(blocks2, new BlockJoinKeyGenerator())
                .map(new RecordMapper<Pair<Block, Block>, Long>() {

                    @Override
                    public void mapRecord(
                            Pair<ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.Block, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.Block> record,
                            DataIterator<Long> resultCollector) {
                        resultCollector.next(
                                record.getFirst().getSecond().size() * (long) record.getSecond().getSecond().size());
                    }
                });
        List<Long> sizeList = new ArrayList<Long>(blockSizes.sort((l)->l).get()); //.skip(Math.ceil(blockSizes.size() * blockFilterRatio)).take(1).firstOrNull();
        int filterIdx = (int)Math.floor(sizeList.size() * blockFilterRatio)-1;
        System.out.println(String.format("Keeping %d/%d blocks (%.2f block filter ratio)", filterIdx, sizeList.size(), blockFilterRatio));
        // for(int i = 0; i < sizeList.size(); i++) {
        //     Long l = sizeList.get(i);
        //     double percent = i / (double)sizeList.size();
        //     if(i<filterIdx) {
        //         System.out.println(String.format("%.2f: %d", percent, l));
        //     } else {
        //         System.out.println(String.format("%.2f filtered: %d", percent, l));
        //     }
        // }
        Long firstFilteredBlockSize = sizeList.get(filterIdx);

        if(firstFilteredBlockSize!=null) {
            maxBlockSize = Math.min(maxBlockSize, firstFilteredBlockSize);
        }
        final long effectiveMaxBlockSize = maxBlockSize;
        logger.info(String.format("Block Filtering: removing blocks with more than %,d pairs", effectiveMaxBlockSize));

        logger.info("Hashing dataset2");
        // Map<String, Set<Pair<BlockedType, BlockingVector>>> hashed = Pair.toMap(blocks2.get());
        Map<String, Set<Pair<BlockedType, BlockingVector>>> hashed = new HashMap<>();
		
		for(Block b : blocks2.get()) {
			hashed.put(b.getFirst(), b.getSecond());
        }
        System.out.println(String.format("Hashed %d tokens", hashed.size()));
        
        logger.info("Hash-Join");
        // hash-join blocks1 with blocks2
        // collect pairs into an aggregate collector that groups by record-pair and aggregates the vectors into a similarity
        Processable<Pair<Pair<Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>, Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>>, Double>> aggregated = blocks1
                .aggregate(
                        new RecordKeyValueMapper<Pair<Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>, Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>>, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.Block, Double>() {

                            @Override
                            public void mapRecordToKey(
                                    ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.Block record,
                                    DataIterator<Pair<Pair<Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>, Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>>, Double>> resultCollector) {

                                String currentToken = record.getFirst();

                                // get the matching block for the right dataset
                                Set<Pair<BlockedType, BlockingVector>> right = hashed.get(currentToken);

                                if (right != null) {
                                    Long blockSize = record.getSecond().size() * (long) right.size();

                                    if(blockSize <= effectiveMaxBlockSize) {
                                        // for each record in the current block (left dataset)
                                        for (Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector> p : record
                                                .getSecond()) {
                                            // BlockedType leftRecord = p.getFirst();
                                            
                                            // for each record in the right block
                                            for (Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector> p2 : right) {
                                                // BlockedType rightRecord = p2.getFirst();
                                                double score = similarityFunction.calculateDimensionScore(
                                                        p.getSecond().get(currentToken), p2.getSecond().get(currentToken));

                                                // important: use LeftIdentityPair here to prevent hash calculation for each BlockingVector!
                                                resultCollector.next(new LeftIdentityPair<>(
                                                    new Pair<>(
                                                        new LeftIdentityPair<>(p.getFirst(), p.getSecond()), 
                                                        new LeftIdentityPair<>(p2.getFirst(), p2.getSecond())), 
                                                    score));
                                            }
                                        }
                                    } else {
                                        // System.out.println(String.format("Block for token '%s' filtered out", currentToken));
                                    }
                                } else {
                                    // System.out.println(String.format("No match for token '%s'", currentToken));
                                }
                            }
                        },
                        new DataAggregator<Pair<Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>, Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>>, Double, Double>() {

                            @Override
                            public Pair<Double, Object> initialise(
                                    Pair<Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>, Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>> keyValue) {
                                return stateless(0.0);
                            }

                            @Override
                            public Pair<Double, Object> aggregate(Double previousResult, Double record, Object state) {

                                double score = similarityFunction.aggregateDimensionScores(previousResult, record);

                                return stateless(score);
                            }

                            @Override
                            public Pair<Double, Object> merge(Pair<Double, Object> intermediateResult1,
                                    Pair<Double, Object> intermediateResult2) {

                                double score = similarityFunction.aggregateDimensionScores(
                                        intermediateResult1.getFirst(), intermediateResult2.getFirst());

                                return stateless(score);
                            }

                            public Double createFinalValue(
                                    Pair<Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>, Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>> keyValue,
                                    Double result, Object state) {

                                BlockedType record1 = keyValue.getFirst().getFirst();
                                BlockedType record2 = keyValue.getSecond().getFirst();

                                BlockingVector leftVector = keyValue.getFirst().getSecond();
                                BlockingVector rightVector = keyValue.getSecond().getSecond();

                                double similarityScore = 0.0;
                                if(recalculateScores) {
                                    for(String token : leftVector.keySet()) {
                                        Double s1 = leftVector.get(token);
                                        Double s2 = rightVector.get(token);
                                        if(s1!=null && s2!=null) {
                                            similarityScore += similarityFunction.calculateDimensionScore(s1, s2);
                                        }
                                    }
                                } else {
                                    similarityScore = result;
                                }

                                similarityScore = similarityFunction.normaliseScore(result, leftVector, rightVector);

                                if (similarityScore >= similarityThreshold) {
                                    return similarityScore;
                                } else {
                                    return null;
                                }
                            }
                        });

        System.out.println(String.format("Join resulted in %d matches", aggregated.size()));

        logger.info("Creating Correspondences");
        return aggregated.map(new RecordMapper<Pair<Pair<Pair<BlockedType,BlockingVector>,Pair<BlockedType,BlockingVector>>,Double>,Correspondence<BlockedType, CorrespondenceType>>() {

                    @Override
                    public void mapRecord(
                            Pair<Pair<Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>, Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>>, Double> record,
                            DataIterator<Correspondence<BlockedType, CorrespondenceType>> resultCollector) {
                        if(record.getSecond()!=null) {
                            resultCollector.next(new Correspondence<>(record.getFirst().getFirst().getFirst(), record.getFirst().getSecond().getFirst(), record.getSecond()));
                        }
                    }
        });


        

        // final AggregateCollector<Pair<BlockedType,BlockedType>, OutputRecordType, ResultType> aggregateCollector = new ThreadSafeAggregateCollector<>();
		
		// aggregateCollector.setAggregator(aggregator);
		// aggregateCollector.initialise();
		
		// new Parallel<Collection<RecordType>>().tryForeach(partitionRecords(), new Consumer<Collection<RecordType>>() {

		// 	@Override
		// 	public void execute(Collection<RecordType> parameter) {
		// 		for(RecordType record : parameter) {
		// 			groupBy.mapRecordToKey(record, aggregateCollector);
		// 		}
		// 	}
		// }, String.format("ParallelProcessableCollection.aggregate: %d elements", size()));
		
		// aggregateCollector.finalise();
		
        // return aggregateCollector.getAggregationResult();
        




		// // create pairs (contains duplicates)
		// logger.info("Creating record pairs");
		// Processable<Triple<String, BlockedType, BlockedType>> pairs = blocks1.join(blocks2, new BlockJoinKeyGenerator())
		// 		.map((Pair<ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.Block, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.Block> record,
		// 				DataIterator<Triple<String, BlockedType, BlockedType>> resultCollector) -> {

		// 			Block leftBlock = record.getFirst();
		// 			Block rightBlock = record.getSecond();

		// 			for (BlockedType leftRecord : leftBlock.getSecond()) {
		// 				for (BlockedType rightRecord : rightBlock.getSecond()) {

		// 					resultCollector.next(new Triple<>(record.getFirst().getFirst(), leftRecord, rightRecord));

		// 				}
		// 			}
		// 		});

		// if (this.isMeasureBlockSizes()) {
		// 	measureBlockSizes(pairs);
		// }

		// // join pairs with vectors on BlockedType
		// logger.info("Joining record pairs with vectors");
		// Processable<Triple<String, Pair<BlockedType, BlockingVector>, Pair<BlockedType, BlockingVector>>> pairsWithVectors = pairs
		// 		.join(vectors1, (t) -> t.getSecond(), (p) -> p.getFirst())
		// 		.join(vectors2, (p) -> p.getFirst().getThird(), (p) -> p.getFirst())
		// 		.map((Pair<Pair<Triple<String, BlockedType, BlockedType>, Pair<BlockedType, BlockingVector>>, Pair<BlockedType, BlockingVector>> record,
		// 				DataIterator<Triple<String, Pair<BlockedType, BlockingVector>, Pair<BlockedType, BlockingVector>>> resultCollector) -> {
		// 			resultCollector.next(new Triple<>(record.getFirst().getFirst().getFirst(),
		// 					record.getFirst().getSecond(), record.getSecond()));
		// 		});

		// // aggregate pairs and create correspondences
		// logger.info("Aggregating record pairs");
		// return createCorrespondences(pairsWithVectors);

	}

    protected void measureBlockSizes(Processable<Block> blocks1, Processable<Block> blocks2) {
		this.initializeBlockingResults();
		int result_id = 0;

		for (Block block : blocks1.get()) {
			Record model = new Record(Integer.toString(result_id));
			model.setValue(AbstractBlocker.blockingKeyValue, block.getFirst().toString());
			model.setValue(AbstractBlocker.frequency, Integer.toString(block.getSecond().size()));
			result_id += 1;
			
			this.appendBlockingResult(model);
        }
        for (Block block : blocks2.get()) {
			Record model = new Record(Integer.toString(result_id));
			model.setValue(AbstractBlocker.blockingKeyValue, block.getFirst().toString());
			model.setValue(AbstractBlocker.frequency, Integer.toString(block.getSecond().size()));
			result_id += 1;
			
			this.appendBlockingResult(model);
		}
	}

	// protected void measureBlockSizes(Processable<Triple<String, BlockedType, BlockedType>> pairs) {
	// 	// calculate block size distribution
	// 	Processable<Pair<String, Integer>> aggregated = pairs
	// 			.aggregate((Triple<String, BlockedType, BlockedType> record,
	// 					DataIterator<Pair<String, Integer>> resultCollector) -> {
	// 				resultCollector.next(new Pair<String, Integer>(record.getFirst(), 1));
	// 			}, new CountAggregator<>());

	// 	this.initializeBlockingResults();
	// 	int result_id = 0;

	// 	for (Pair<String, Integer> value : aggregated.sort((v) -> v.getSecond(), false).get()) {
	// 		Record model = new Record(Integer.toString(result_id));
	// 		model.setValue(AbstractBlocker.blockingKeyValue, value.getFirst().toString());
	// 		model.setValue(AbstractBlocker.frequency, value.getFirst().toString());
	// 		result_id += 1;
			
	// 		this.appendBlockingResult(model);
	// 	}
	// }

	protected Processable<Pair<BlockedType, BlockingVector>> createBlockingVectors(
			Processable<Pair<RecordType, Processable<Correspondence<CorrespondenceType, Matchable>>>> ds,
			BlockingKeyGenerator<RecordType, CorrespondenceType, BlockedType> blockingFunction) {

		// input: a dataset of records
		return ds.aggregate(
				new RecordKeyValueMapper<BlockedType, Pair<RecordType, Processable<Correspondence<CorrespondenceType, Matchable>>>, Pair<String, Processable<Correspondence<CorrespondenceType, Matchable>>>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void mapRecordToKey(
							Pair<RecordType, Processable<Correspondence<CorrespondenceType, Matchable>>> record,
							DataIterator<Pair<BlockedType, Pair<String, Processable<Correspondence<CorrespondenceType, Matchable>>>>> resultCollector) {

						// apply the blocking key generator to the current
						// record
						Processable<Pair<RecordType, Processable<Correspondence<CorrespondenceType, Matchable>>>> col = new ProcessableCollection<>();
						col.add(record);
						Processable<Pair<String, Pair<BlockedType, Processable<Correspondence<CorrespondenceType, Matchable>>>>> blockingKeyValues = col
								.map(blockingFunction);

						// then create pairs of (blocking key value,
						// correspondences) and group them by the blocked
						// element
						for (Pair<String, Pair<BlockedType, Processable<Correspondence<CorrespondenceType, Matchable>>>> p : blockingKeyValues
								.get()) {
							BlockedType blocked = p.getSecond().getFirst();
							String blockingKeyValue = p.getFirst();
							Processable<Correspondence<CorrespondenceType, Matchable>> correspondences = p.getSecond()
									.getSecond();
							resultCollector.next(new Pair<>(blocked, new Pair<>(blockingKeyValue, correspondences)));
						}
					}
				},
				// aggregate the blocking key values for each blocked element
				// into blocking vectors
				new DataAggregator<BlockedType, Pair<String, Processable<Correspondence<CorrespondenceType, Matchable>>>, BlockingVector>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Pair<BlockingVector, Object> initialise(BlockedType keyValue) {
						return stateless(new BlockingVector());
					}

					@Override
					public Pair<BlockingVector, Object> aggregate(BlockingVector previousResult,
							Pair<String, Processable<Correspondence<CorrespondenceType, Matchable>>> record,
							Object state) {

						// get the dimension for the current blocking key value
						// in the blocking vector
						Double existing = previousResult.get(record.getFirst());

						if (existing == null) {
							// existing = new Pair<Double,
							// Processable<Correspondence<CorrespondenceType,Matchable>>>(0.0,
							// new ProcessableCollection<>());
							existing = 0.0;

						}

						// increment the frequency for this blocking key value
						Double frequency = existing + 1;

						// existing = new Pair<Double,
						// Processable<Correspondence<CorrespondenceType,Matchable>>>(frequency,
						// existing.getSecond().append(record.getSecond()));
						existing = frequency;

						previousResult.put(record.getFirst(), existing);
						previousResult.addCorrespondences(record.getSecond());
						return stateless(previousResult);
					}

					/*
					 * (non-Javadoc)
					 * 
					 * @see de.uni_mannheim.informatik.dws.winter.processing.
					 * DataAggregator#merge(de.uni_mannheim.informatik.dws.
					 * winter.model.Pair,
					 * de.uni_mannheim.informatik.dws.winter.model.Pair)
					 */
					@Override
					public Pair<ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector, Object> merge(
							Pair<ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector, Object> intermediateResult1,
							Pair<ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector, Object> intermediateResult2) {

						BlockingVector first = intermediateResult1.getFirst();
						BlockingVector second = intermediateResult2.getFirst();

						Set<String> keys = Q.union(first.keySet(), second.keySet());

						BlockingVector result = new BlockingVector();
						result.addCorrespondences(first.getCorrespondences());
						result.addCorrespondences(second.getCorrespondences());

						for (String k : keys) {
							// Pair<Double,
							// Processable<Correspondence<CorrespondenceType,
							// Matchable>>> v1 = first.get(k);
							// Pair<Double,
							// Processable<Correspondence<CorrespondenceType,
							// Matchable>>> v2 = second.get(k);

							Double v1 = first.get(k);
							Double v2 = second.get(k);

							if (v1 == null) {
								v1 = v2;
							} else if (v2 != null) {
								// Double f1 = v1.getFirst();
								// Double f2 = v2.getFirst();

								v1 = v1 + v2;

								// v1 = new Pair<Double,
								// Processable<Correspondence<CorrespondenceType,Matchable>>>(frequency,
								// v1.getSecond().append(v2.getSecond()));
							}

							result.put(k, v1);
						}

						return stateless(result);
					}

					/*
					 * (non-Javadoc)
					 * 
					 * @see de.uni_mannheim.informatik.dws.winter.processing.
					 * DataAggregator#createFinalValue(java.lang.Object,
					 * java.lang.Object)
					 */
					@Override
					public ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector createFinalValue(
							BlockedType keyValue,
							ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector result,
							Object state) {

						BlockingVector vector = new BlockingVector();
						vector.addCorrespondences(result.getCorrespondences());

						for (String s : result.keySet()) {
							// Pair<Double,
							// Processable<Correspondence<CorrespondenceType,
							// Matchable>>> p = result.get(s);
							Double d = result.get(s);

							if (vectorCreationMethod == VectorCreationMethod.BinaryTermOccurrences) {
								// p = new Pair<Double,
								// Processable<Correspondence<CorrespondenceType,Matchable>>>(Math.min(1.0,
								// p.getFirst()), p.getSecond());
								d = Math.min(1.0, d);
							} else {
								// p = new Pair<Double,
								// Processable<Correspondence<CorrespondenceType,Matchable>>>(p.getFirst()
								// / (double)result.size(), p.getSecond());
								d = d / result.size();
							}

							vector.put(s, d);
						}

						return vector;
					}
				});
	}

	protected Processable<Block> createInvertedIndex(Processable<Pair<BlockedType, BlockingVector>> vectors) {

		return vectors.aggregate(
				(Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector> record,
						DataIterator<Pair<String, Pair<BlockedType, BlockingVector>>> resultCollector) -> {

					for (String s : record.getSecond().keySet()) {
						resultCollector.next(new Pair<>(s, record));
					}

				}, new SetAggregator<>()).map((Pair<String, Set<Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>>> record,
						DataIterator<ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.Block> resultCollector) -> {

					resultCollector.next(new Block(record.getFirst(), record.getSecond()));
					;

				});

	}

	protected Processable<Pair<String, Double>> createDocumentFrequencies(Processable<Block> blocks1,
			Processable<Block> blocks2, DocumentFrequencyCounter documentFrequencyCounter) {

		// calculate document frequencies
		Processable<Pair<String, Double>> df1 = blocks1
				.map((ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.Block record,
						DataIterator<Pair<String, Double>> resultCollector) -> {
					resultCollector.next(new Pair<>(record.getFirst(), (double) record.getSecond().size()));
				});

		Processable<Pair<String, Double>> df2 = blocks2
				.map((ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.Block record,
						DataIterator<Pair<String, Double>> resultCollector) -> {
					resultCollector.next(new Pair<>(record.getFirst(), (double) record.getSecond().size()));
				});

		Processable<Pair<String, Double>> df = null;

		switch(documentFrequencyCounter) {
			case Dataset1:
				df = df1;
				break;
			case Dataset2:
				df = df2;
				break;
			default:
				df = df1.append(df2);
		}

		return df
			.aggregate((Pair<String, Double> record, DataIterator<Pair<String, Double>> resultCollector) -> {
				resultCollector.next(record);
			}, new SumDoubleAggregator<>());
	}

	protected int getDocumentCount(Processable<Pair<BlockedType, BlockingVector>> vectors1, Processable<Pair<BlockedType, BlockingVector>> vectors2, DocumentFrequencyCounter documentFrequencyCounter) {
		switch(documentFrequencyCounter) {
			case Dataset1:
				return vectors1.size();
			case Dataset2:
				return vectors2.size();
			default:
				return vectors1.size() + vectors2.size();
		}
	}

	protected Processable<Pair<String, Double>> createIDF(Processable<Pair<String, Double>> documentFrequencies, int documentCount) {
		return documentFrequencies.map((f)->new Pair<String, Double>(f.getFirst(), Math.log(documentCount / f.getSecond())));
	}

	protected Processable<Pair<BlockedType, BlockingVector>> createTFIDFVectors(
			Processable<Pair<BlockedType, BlockingVector>> vectors,
			// Processable<Pair<String, Double>> documentFrequencies, int documentCount) {
			Processable<Pair<String, Double>> inverseDocumentFrequencies) {

		// Map<String, Double> dfMap = Q.map(documentFrequencies.get(), (p) -> p.getFirst(), (p) -> p.getSecond());
		Map<String, Double> idfMap = Q.map(inverseDocumentFrequencies.get(), (p) -> p.getFirst(), (p) -> p.getSecond());

		return vectors.map((
				Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector> record,
				DataIterator<Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>> resultCollector) -> {
			BlockingVector tfVector = record.getSecond();
			BlockingVector tfIdfVector = new BlockingVector();

			for (String s : tfVector.keySet()) {
				Double tfScore = tfVector.get(s);

				// double df = dfMap.get(s);
				// double tfIdfScore = tfScore * Math.log(documentCount / df);
				Double idf = idfMap.get(s);
				if(idf==null) {
					idf = 0.0;
				}
				double tfIdfScore = tfScore * idf;

				tfIdfVector.put(s, tfIdfScore);
			}

			resultCollector.next(new Pair<>(record.getFirst(), tfIdfVector));
		});

	}

	protected Processable<Correspondence<BlockedType, CorrespondenceType>> createCorrespondences(
			Processable<Triple<String, Pair<BlockedType, BlockingVector>, Pair<BlockedType, BlockingVector>>> pairsWithVectors) {
		return pairsWithVectors.aggregate((
				Triple<String, Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>, Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>> record,
				DataIterator<Pair<Pair<Pair<BlockedType, BlockingVector>, Pair<BlockedType, BlockingVector>>, Pair<Double, Double>>> resultCollector) -> {
			String dimension = record.getFirst();

			BlockedType leftRecord = record.getSecond().getFirst();
			BlockedType rightRecord = record.getThird().getFirst();

			BlockingVector leftVector = record.getSecond().getSecond();
			BlockingVector rightVector = record.getThird().getSecond();

			Pair<Pair<BlockedType, BlockingVector>, Pair<BlockedType, BlockingVector>> key = new Pair<>(
					new LeftIdentityPair<>(leftRecord, leftVector), new LeftIdentityPair<>(rightRecord, rightVector));
			Pair<Double, Double> value = new Pair<>(leftVector.get(dimension), rightVector.get(dimension));

			resultCollector.next(new Pair<>(key, value));
		}, new DataAggregator<Pair<Pair<BlockedType, BlockingVector>, Pair<BlockedType, BlockingVector>>, Pair<Double, Double>, Correspondence<BlockedType, CorrespondenceType>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Pair<Correspondence<BlockedType, CorrespondenceType>, Object> initialise(
					Pair<Pair<BlockedType, BlockingVector>, Pair<BlockedType, BlockingVector>> keyValue) {
				return stateless(
						new Correspondence<>(keyValue.getFirst().getFirst(), keyValue.getSecond().getFirst(), 0.0));
			}

			@Override
			public Pair<Correspondence<BlockedType, CorrespondenceType>, Object> aggregate(
					Correspondence<BlockedType, CorrespondenceType> previousResult, Pair<Double, Double> record,
					Object state) {

				Double leftEntry = record.getFirst();
				Double rightEntry = record.getSecond();

				double score = similarityFunction.calculateDimensionScore(leftEntry, rightEntry);

				score = similarityFunction.aggregateDimensionScores(previousResult.getSimilarityScore(), score);

				return stateless(new Correspondence<BlockedType, CorrespondenceType>(previousResult.getFirstRecord(),
						previousResult.getSecondRecord(), score, null));
			}

			@Override
			public Pair<Correspondence<BlockedType, CorrespondenceType>, Object> merge(
					Pair<Correspondence<BlockedType, CorrespondenceType>, Object> intermediateResult1,
					Pair<Correspondence<BlockedType, CorrespondenceType>, Object> intermediateResult2) {

				Correspondence<BlockedType, CorrespondenceType> c1 = intermediateResult1.getFirst();
				Correspondence<BlockedType, CorrespondenceType> c2 = intermediateResult2.getFirst();

				Correspondence<BlockedType, CorrespondenceType> result = new Correspondence<>(c1.getFirstRecord(),
						c1.getSecondRecord(),
						similarityFunction.aggregateDimensionScores(c1.getSimilarityScore(), c2.getSimilarityScore()));

				return stateless(result);
			}

			public Correspondence<BlockedType, CorrespondenceType> createFinalValue(
					Pair<Pair<BlockedType, BlockingVector>, Pair<BlockedType, BlockingVector>> keyValue,
					Correspondence<BlockedType, CorrespondenceType> result, Object state) {

				BlockedType record1 = keyValue.getFirst().getFirst();
				BlockedType record2 = keyValue.getSecond().getFirst();

				BlockingVector leftVector = keyValue.getFirst().getSecond();
				BlockingVector rightVector = keyValue.getSecond().getSecond();

				double similarityScore = similarityFunction.normaliseScore(result.getSimilarityScore(), leftVector,
						rightVector);

				if (similarityScore >= similarityThreshold) {
					Processable<Correspondence<CorrespondenceType, Matchable>> causes = createCausalCorrespondences(
							record1, record2, leftVector, rightVector);

					return new Correspondence<>(result.getFirstRecord(), result.getSecondRecord(), similarityScore,
							causes);
				} else {
					return null;
				}
			}
		}).map((Pair<Pair<Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>, Pair<BlockedType, ImprovedBlockingKeyIndexer<RecordType, SchemaElementType, BlockedType, CorrespondenceType>.BlockingVector>>, Correspondence<BlockedType, CorrespondenceType>> record,
				DataIterator<Correspondence<BlockedType, CorrespondenceType>> resultCollector) -> {
			resultCollector.next(record.getSecond());
		});
	}

	protected Processable<Correspondence<CorrespondenceType, Matchable>> createCausalCorrespondences(
			BlockedType record1, BlockedType record2, BlockingVector vector1, BlockingVector vector2) {

		Processable<Correspondence<CorrespondenceType, Matchable>> causes = new ProcessableCollection<>(
				vector1.getCorrespondences().get()).append(vector2.getCorrespondences()).distinct();

		int[] pairIds = new int[] { record1.getDataSourceIdentifier(), record2.getDataSourceIdentifier() };
		Arrays.sort(pairIds);

		// filter the correspondences such that only correspondences between the
		// two records are contained (by data source id)
		return causes.where((c) -> {

			int[] causeIds = new int[] { c.getFirstRecord().getDataSourceIdentifier(),
					c.getSecondRecord().getDataSourceIdentifier() };
			Arrays.sort(causeIds);

			return Arrays.equals(pairIds, causeIds);
		});
	}
}
