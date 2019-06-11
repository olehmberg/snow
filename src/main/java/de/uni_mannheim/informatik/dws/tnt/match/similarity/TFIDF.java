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
package de.uni_mannheim.informatik.dws.tnt.match.similarity;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer.VectorCreationMethod;
import de.uni_mannheim.informatik.dws.winter.model.LeftIdentityPair;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SetAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SumDoubleAggregator;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 * @param <DataType>			the datatype
 */
public class TFIDF<DataType>
{
	protected class IndexEntry extends LeftIdentityPair<String, Set<DataType>> {
		private static final long serialVersionUID = 1L;

		public IndexEntry(String first, Set<DataType> second) {
			super(first, second);
		}
	}


    private Function<String[], DataType> getStringTokensFunction;

	private VectorCreationMethod vectorCreationMethod;

	
	public TFIDF(
        Function<String[], DataType> getStringTokens, 
        VectorCreationMethod vectorCreationMethod
    ) {
		this.getStringTokensFunction = getStringTokens;
		this.vectorCreationMethod = vectorCreationMethod;
    }
    
    private Processable<Pair<DataType, TokenVector<String>>> vectors;
    private Processable<IndexEntry> invertedIndex;
    private Processable<Pair<String, Double>> documentFrequencies;
    private Map<String, Double> dfMap;
    private int documentCount;
    private Map<String, Integer> tokenIndices;
	
	public <RecordType> Processable<Pair<DataType, TokenVector<String>>> prepare(
			Processable<RecordType> dataset, RecordMapper<RecordType, DataType> valueSelector) {
        Processable<DataType> data = dataset.map(valueSelector);

		// create blocking key value vectors
		System.out.println("[TFIDF] Creating document vectors");
        vectors = createTokenVectors(data, getStringTokensFunction);
        System.out.println(String.format("[TFIDF] Created %d document vectors", vectors.size()));
		
		// create inverted index
		System.out.println("[TFIDF] Creating inverted index");
        invertedIndex = createInvertedIndex(vectors);
        createTokenIndices();
        System.out.println(String.format("[TFIDF] Created inverted index with %d unique tokens", invertedIndex.size()));
		
		if(vectorCreationMethod==VectorCreationMethod.TFIDF) {
            System.out.println("[TFIDF] Calculating document frequencies");
            // calculate document frequencies
            documentFrequencies = createDocumentFrequencies(invertedIndex);
            
            dfMap = Q.map(documentFrequencies.get(), (p)->p.getFirst(), (p)->p.getSecond());
            
            System.out.println("[TFIDF] Calculating TFIDF vectors");
            documentCount = vectors.size();
            // update token  vectors with TF-IDF weights
			vectors = createTFIDFVectors(vectors, documentFrequencies, documentCount);
        }
        
        return vectors;
    }

    protected void createTokenIndices() {
        tokenIndices = new HashMap<>();

        int index = 0;
        for(IndexEntry i : invertedIndex.get()) {
            tokenIndices.put(i.getFirst(), index++);
        }
    }

    public Processable<Pair<DataType, TokenVector<String>>> createVectors(
        Processable<DataType> dataset) {
            Processable<Pair<DataType, TokenVector<String>>> newVectors = createTokenVectors(dataset, getStringTokensFunction);
            if(vectorCreationMethod==VectorCreationMethod.TFIDF) {
                // update token  vectors with TF-IDF weights
                newVectors = createTFIDFVectors(newVectors, documentFrequencies, documentCount);
            }
            return newVectors;
    }

    public TokenVector<String> createVector(DataType record) {
        ProcessableCollection<DataType> col = new ProcessableCollection<>();
        col.add(record);
        Pair<DataType, TokenVector<String>> p = createVectors(col).firstOrNull();
        if(p==null) {
            return null;
        } else {
            return p.getSecond();
        }
    }

    public double calculateSimilarity(DataType record1, DataType record2, VectorSpaceSimilarity similarityFunction) {
        TokenVector<String> v1 = createVector(record1);
        TokenVector<String> v2 = createVector(record2);

        // System.out.println(String.format("%s: (%s)", record1.toString(), v1.format()));

        return calculateSimilarity(v1,v2,similarityFunction);
    }

    public double calculateSimilarity(TokenVector<String> record1, TokenVector<String> record2, VectorSpaceSimilarity similarityFunction) {
        Set<String> dimensions = Q.intersection(record1.getDimensions(), record2.getDimensions());


        double score = 0.0;
        for(String token : dimensions) {
            double dimScore = similarityFunction.calculateDimensionScore(record1.get(token), record2.get(token));
            
            score = similarityFunction.aggregateDimensionScores(score, dimScore);
        }

        score = similarityFunction.normaliseScore(score, record1.toMap(), record2.toMap());

        return score;
    }

    protected Processable<Pair<DataType, TokenVector<String>>> createTokenVectors(
			Processable<DataType> ds, 
			Function<String[], DataType> getStringTokens) {
		
		// input: a dataset of records
        return ds
                // create the tokens
                .aggregate((r,col)->{for(String s : getStringTokens.execute(r)) { col.next(new Pair<>(r, s)); }},
                // aggregate the tokens for each record into token vectors
				new DataAggregator<DataType, String, TokenVector<String>>() {
		
					private static final long serialVersionUID = 1L;
		
					@Override
					public Pair<TokenVector<String>,Object> initialise(
                        DataType keyValue) {
						return stateless(new TokenVector<String>());
					}
		
					@Override
					public Pair<TokenVector<String>,Object> aggregate(
                        TokenVector<String> previousResult,
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
						previousResult.set(record, existing);
						return stateless(previousResult);
					}
					
					/* (non-Javadoc)
					 * @see de.uni_mannheim.informatik.dws.winter.processing.DataAggregator#merge(de.uni_mannheim.informatik.dws.winter.model.Pair, de.uni_mannheim.informatik.dws.winter.model.Pair)
					 */
					@Override
					public Pair<TokenVector<String>, Object> merge(
							Pair<TokenVector<String>, Object> intermediateResult1,
							Pair<TokenVector<String>, Object> intermediateResult2) {
						
                                TokenVector<String> first = intermediateResult1.getFirst();
                                TokenVector<String> second = intermediateResult2.getFirst();
						
						Set<String> keys = Q.union(first.getDimensions(), second.getDimensions());
						
						TokenVector<String> result = new TokenVector<String>();
						
						for(String k : keys) {
							Double v1 = first.get(k);
							Double v2 = second.get(k);
							
							if(v1==null) {
								v1 = v2;
							} else if(v2!=null) {
								v1 = v1 + v2;
							}
							
							result.set(k, v1);
						}
						
						return stateless(result);
					}
					
					/* (non-Javadoc)
					 * @see de.uni_mannheim.informatik.dws.winter.processing.DataAggregator#createFinalValue(java.lang.Object, java.lang.Object)
					 */
					@Override
					public TokenVector<String> createFinalValue(
                        DataType keyValue,
                        TokenVector<String> result,
							Object state) {
						
                                TokenVector<String> vector = new TokenVector<String>();
						
						for(String s : result.getDimensions()) {
							Double d = result.get(s);
							
							if(vectorCreationMethod==VectorCreationMethod.BinaryTermOccurrences) {
								d = Math.min(1.0, d);
							} else if(vectorCreationMethod==VectorCreationMethod.TermFrequencies) {
								d = d / result.getDimensions().size();
							}
							
							vector.set(s, d);
						}
						
						return vector;
					}
				});
	}

	protected Processable<IndexEntry> createInvertedIndex(Processable<Pair<DataType, TokenVector<String>>> vectors) {
		
		return vectors
            .aggregate((Pair<DataType, TokenVector<String>> record, DataIterator<Pair<String, DataType>> resultCollector) 
            -> {
                
                for(String s : record.getSecond().getDimensions()) {
                    resultCollector.next(new Pair<>(s, record.getFirst()));
                }
                
            }, 
            new SetAggregator<>())
            .map((Pair<String, Set<DataType>> record,DataIterator<IndexEntry> resultCollector) 
            -> {
                resultCollector.next(new IndexEntry(record.getFirst(), record.getSecond()));;	
            });
		
	}
	
	protected Processable<Pair<String, Double>> createDocumentFrequencies(Processable<IndexEntry> invertedIndex) {
		
		// calculate document frequencies
		Processable<Pair<String, Double>> df = invertedIndex
            .map((IndexEntry record,DataIterator<Pair<String, Double>> resultCollector) 
                -> {
                    resultCollector.next(new Pair<>(record.getFirst(), (double)record.getSecond().size()));
                });

		
		return df
			.aggregate((Pair<String, Double> record, DataIterator<Pair<String, Double>> resultCollector) 
            -> {
                resultCollector.next(record);
            }
            , new SumDoubleAggregator<>());
	}
	
	protected Processable<Pair<DataType, TokenVector<String>>> createTFIDFVectors(Processable<Pair<DataType, TokenVector<String>>> vectors, Processable<Pair<String, Double>> documentFrequencies, int documentCount) {
		return vectors
            .map((Pair<DataType, TokenVector<String>> record, DataIterator<Pair<DataType, TokenVector<String>>> resultCollector) 
            -> {
                TokenVector<String> tfVector = record.getSecond();
                // TokenVector<String> tfIdfVector = new FixedTokenVector(tokenIndices, indexToToken);
                TokenVector<String> tfIdfVector = new TokenVector<>();
                
                for(String s : tfVector.getDimensions()) {
                    if(tokenIndices.containsKey(s)) {   // skip tokens for which no examples were seen in preprocessing (and we have no document frequencies)
                        Double tfScore = tfVector.get(s);;
                        Double df = dfMap.get(s);
                        if(df==null) {
                            df=1.0;
                        }
                        double tfIdfScore = tfScore * Math.log( documentCount / df );
                        tfIdfVector.set(s, tfIdfScore);
                    }
                }
                
                resultCollector.next(new Pair<>(record.getFirst(), tfIdfVector));
            });
		
	}
	
}
