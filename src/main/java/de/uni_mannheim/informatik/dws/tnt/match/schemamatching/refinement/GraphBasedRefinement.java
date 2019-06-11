/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.utils.query.P;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import edu.uci.ics.jung.algorithms.cluster.WeakComponentClusterer;
import edu.uci.ics.jung.algorithms.importance.BetweennessCentrality;
import edu.uci.ics.jung.algorithms.shortestpath.DijkstraShortestPath;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class GraphBasedRefinement {

	private boolean verbose = false;
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	private File logDirectory;

	public void setLogDirectory(File f) {
		logDirectory = f;
	}

	private boolean returnInvalidCorrespondences = false;
	public void setReturnInvalidCorrespondences(boolean returnInvalidCorrespondences) {
		this.returnInvalidCorrespondences = returnInvalidCorrespondences;
	}
	
	// when removing an edge, remove all other edges with the same labels, too
	private boolean useLabelPropagation = false;
	
	public GraphBasedRefinement(boolean useLabelPropagation) {
		this.useLabelPropagation = useLabelPropagation;
	}
	
	public Processable<Correspondence<MatchableTableColumn, Matchable>> match(
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
			final DisjointHeaders dh) {
		
		// runtime improvement: create one graph per connected component, process all components in parallel (calculate betweenness, remove conflicts)
		
		ConnectedComponentClusterer<MatchableTableColumn> clusterer = new ConnectedComponentClusterer<>();
		for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
			clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
		}
		
		Map<Collection<MatchableTableColumn>, MatchableTableColumn> clusters = clusterer.createResult();
		
		Processable<Pair<MatchableTableColumn, Integer>> clustering = new ProcessableCollection<>();
		int cluIdx = 0;
		for(Collection<MatchableTableColumn> cluster : clusters.keySet()) {
			for(MatchableTableColumn c : cluster) {
				clustering.add(new Pair<MatchableTableColumn, Integer>(c, cluIdx));
			}
			cluIdx++;
		}
		
		Function<MatchableTableColumn, Correspondence<MatchableTableColumn, Matchable>> joinByLeftColumn = new Function<MatchableTableColumn, Correspondence<MatchableTableColumn,Matchable>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public MatchableTableColumn execute(Correspondence<MatchableTableColumn, Matchable> input) {
				return input.getFirstRecord();
			}
		};
		Function<MatchableTableColumn, Pair<MatchableTableColumn, Integer>> joinByClusteredColumn = new Function<MatchableTableColumn, Pair<MatchableTableColumn,Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public MatchableTableColumn execute(Pair<MatchableTableColumn, Integer> input) {
				return input.getFirst();
			}
		};
		
		System.out.println("[GraphBasedRefinement] joining correspondences with clusters");
		Processable<Pair<Correspondence<MatchableTableColumn, Matchable>, Pair<MatchableTableColumn, Integer>>> corsWithClusters = schemaCorrespondences.join(clustering, joinByLeftColumn, joinByClusteredColumn);
		
		RecordKeyValueMapper<Integer, Pair<Correspondence<MatchableTableColumn, Matchable>, Pair<MatchableTableColumn, Integer>>, Correspondence<MatchableTableColumn, Matchable>> groupByCluster = new RecordKeyValueMapper<Integer, Pair<Correspondence<MatchableTableColumn,Matchable>,Pair<MatchableTableColumn,Integer>>, Correspondence<MatchableTableColumn,Matchable>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecordToKey(
					Pair<Correspondence<MatchableTableColumn, Matchable>, Pair<MatchableTableColumn, Integer>> record,
					DataIterator<Pair<Integer, Correspondence<MatchableTableColumn, Matchable>>> resultCollector) {

				resultCollector.next(new Pair<Integer, Correspondence<MatchableTableColumn,Matchable>>(record.getSecond().getSecond(), record.getFirst()));
				
			}
		};
		System.out.println("[GraphBasedRefinement] grouping correspondences by clusters");
		Processable<Group<Integer, Correspondence<MatchableTableColumn, Matchable>>> groupedByCluster = corsWithClusters.group(groupByCluster);
		
		RecordMapper<Group<Integer, Correspondence<MatchableTableColumn, Matchable>>, Correspondence<MatchableTableColumn, Matchable>> resolveConflictsTransformation = new RecordMapper<Group<Integer,Correspondence<MatchableTableColumn,Matchable>>, Correspondence<MatchableTableColumn,Matchable>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<Integer, Correspondence<MatchableTableColumn, Matchable>> record,
					DataIterator<Correspondence<MatchableTableColumn, Matchable>> resultCollector) {

				Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = record.getRecords();
				
				Graph<MatchableTableColumn, Correspondence<MatchableTableColumn, Matchable>> g = new UndirectedSparseGraph<>();
				for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
					g.addEdge(cor, cor.getFirstRecord(), cor.getSecondRecord());
				}
				
				WeakComponentClusterer<MatchableTableColumn, Correspondence<MatchableTableColumn, Matchable>> comp = new WeakComponentClusterer<>();
				Set<Set<MatchableTableColumn>> clusters = comp.apply(g);
				
				BetweennessCentrality<MatchableTableColumn, Correspondence<MatchableTableColumn, Matchable>> between = new BetweennessCentrality<>(g);
				
//				System.out.println("calculating betweenness centrality");
				between.setRemoveRankScoresOnFinalize(false);
				between.evaluate();
				
//				System.out.println("removing conflicts");
				Queue<Set<MatchableTableColumn>> clustersToCheck = new LinkedList<>(clusters); 
				
				Collection<Correspondence<MatchableTableColumn, Matchable>> removedCorrespondences = new LinkedList<>();
				
				// generate conflicts in clusters
				while(clustersToCheck.size()>0) {
					Collection<MatchableTableColumn> cluster = clustersToCheck.poll();
					
					if(verbose) {
						// System.out.println(String.format("[GraphBasedRefinement] Checking cluster of size %d", cluster.size()));
						// for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
						// 	System.out.println(String.format("[GraphBasedRefinement] Edge: %s <-> %s",
						// 		cor.getFirstRecord().toString(),
						// 		cor.getSecondRecord().toString()
						// 	));
						// }
						if(logDirectory!=null) {
							try {
								synchronized(logDirectory) {
									Correspondence.toGraph(schemaCorrespondences.get()).writePajekFormat(new File(logDirectory, "graphbasedrefinement_currentcluster.net"));
								}
							} catch(Exception e) {
								e.printStackTrace();
							}
						}
					}

					Pair<MatchableTableColumn, MatchableTableColumn> c = findConflict(cluster, dh);
					
					boolean cannotResolve = false;

					if(c!=null) {
						StringBuilder sb = new StringBuilder();
						sb.append(String.format("[GraphBasedRefinement] Detected conflict %s<->%s\n", c.getFirst(), c.getSecond()));

						// then remove the edges with the highest betweenness until the cluster breaks into two clusters to resolve the conflict				
						// only remove edges on paths between the nodes that make the conflict
						DijkstraShortestPath<MatchableTableColumn, Correspondence<MatchableTableColumn, Matchable>> dijkstra = new DijkstraShortestPath<>(g);
						
						List<Correspondence<MatchableTableColumn, Matchable>> path = dijkstra.getPath(c.getFirst(), c.getSecond());
						
						do {
							sb.append(String.format("[GraphBasedRefinement]\tShortest Path: %s\n", formatPath(path, between)));
							
							Correspondence<MatchableTableColumn, Matchable> maxEdge = null;
							double maxValue = Double.MIN_VALUE;
							
							Correspondence<MatchableTableColumn, Matchable> maxContextEdge = null;
							double maxContextValue = Double.MIN_VALUE;
							
							// find the correspondence with the highest betweenness value
							for(Correspondence<MatchableTableColumn, Matchable> cor : path) {
								
								// but don't remove a correspondence between columns with the same header, unless the conflict is in a single table
								String header1 = cor.getFirstRecord().getHeader();
								String header2 = cor.getSecondRecord().getHeader();
								boolean equalHeaders = header1!=null && header1.equals(header2);
								
								// check for same table per correspondence on the conflicting path
								boolean isSameTable = cor.getFirstRecord().getTableId() == cor.getSecondRecord().getTableId();
								
								if(cluster.contains(cor.getFirstRecord()) && (!equalHeaders || isSameTable)) {
									
									double value = between.getEdgeRankScore(cor);
									
									if(value>maxValue) {
										maxValue = value;
										maxEdge = cor;
									}
									
									if(ContextColumns.isContextColumn(cor.getFirstRecord()) || ContextColumns.isContextColumn(cor.getSecondRecord())) {
										if(value>maxContextValue) {
											maxContextValue = value;
											maxContextEdge = cor;
										}
									}
									
								}
								
							}
							
							// if there is a correspondence involving a context column, remove that one first
							if(maxContextEdge!=null) {
								maxEdge = maxContextEdge;
							}
							
							if(maxEdge!=null) {

								Collection<Correspondence<MatchableTableColumn, Matchable>> removed = removeEdge(schemaCorrespondences, g, maxEdge, sb);
								
								removedCorrespondences.addAll(removed);
								
								// if we could not remove any edge, stop resolving the conflict to avoid an endless loop
								if(removed.size()==0) {
									sb.append(String.format("[GraphBasedRefinement]\tCannot resolve: Could not remove edge %s <-> %s", maxEdge.getFirstRecord().toString(), maxEdge.getSecondRecord().toString()));
									path = null;
									cannotResolve = true;
								} else {
									try {
										dijkstra = new DijkstraShortestPath<>(g);
										path = dijkstra.getPath(c.getFirst(), c.getSecond());
									} catch(Exception ex) {
										path = null;
									}
									
									if(path==null || path.size()==0) {
										sb.append("[GraphBasedRefinement]\tResolved");
										path = null;
									}
								}
							} else {
								sb.append("[GraphBasedRefinement]\tCannot resolve");
								path = null;
								cannotResolve = true;
							}
							
						} while(path!=null); // repeat until there is no conflicting path left
						
						if(verbose) {
							System.out.println(sb.toString());
						}

						// if we couldn't resolve a conflict, don't add the cluster to the queue again!
						if(!cannotResolve) {
							// re-cluster and add the new clusters to the queue
							Set<Set<MatchableTableColumn>> newClusters = comp.apply(g);
							for(Set<MatchableTableColumn> clu : newClusters) {
								// only clusters created from the current one can be changed, as we only removed edges from the current cluster
								if(Q.any(clu, new P.IsContainedIn<>(cluster)) && !cluster.equals(clu)) {
									clustersToCheck.add(clu);
								}
							}
						}
					}
					
					
				}

				
				if(returnInvalidCorrespondences) {
					for(Correspondence<MatchableTableColumn, Matchable> cor : removedCorrespondences) {
						resultCollector.next(cor);
					}
				} else {
					for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
						resultCollector.next(cor);
					}
				}
			}
		};
		
		System.out.println("[GraphBasedRefinement] resolving conflicts");
		System.out.println(String.format("Processable is %s",groupedByCluster.getClass().getName()));
		return groupedByCluster.map(resolveConflictsTransformation);
		
	}
	
	protected Collection<Correspondence<MatchableTableColumn, Matchable>> removeEdge(Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences, Graph<MatchableTableColumn, Correspondence<MatchableTableColumn, Matchable>> g, Correspondence<MatchableTableColumn, Matchable> edge, StringBuilder sb) {
		
		if(!useLabelPropagation) {
			schemaCorrespondences.remove(edge);
			g.removeEdge(edge);
			sb.append(String.format("[GraphBasedRefinement]\tRemoved %s<->%s\n", edge.getFirstRecord(), edge.getSecondRecord()));
			return Q.toList(edge);
		} else {
			String h1 = edge.getFirstRecord().getHeader();
			String h2 = edge.getSecondRecord().getHeader();
			
			Collection<Correspondence<MatchableTableColumn, Matchable>> toRemove = new LinkedList<>();
			
			// if the headers of the conflicting edge are equal or one is a context column, only remove this edge
			if(h1.equals(h2) || ContextColumns.isContextColumn(edge.getFirstRecord()) || ContextColumns.isContextColumn(edge.getSecondRecord())) {
				toRemove.add(edge);
				
			} else { 
			
				// otherwise remove all correspondences with the same combination of column headers
				for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
					if(cor.getFirstRecord().getHeader()!=null && cor.getSecondRecord().getHeader()!=null) {
					
						if(cor.getFirstRecord().getHeader().equals(h1) && cor.getSecondRecord().getHeader().equals(h2)
								|| cor.getFirstRecord().getHeader().equals(h2) && cor.getSecondRecord().getHeader().equals(h1)) {
							toRemove.add(cor);
						}
					
					}
				}

				if(!toRemove.contains(edge)) {
					toRemove.add(edge);
				}
			
			}
			
			for(Correspondence<MatchableTableColumn, Matchable> cor : toRemove) {
				schemaCorrespondences.remove(cor);
				g.removeEdge(cor);
				sb.append(String.format("[GraphBasedRefinement]\tRemoved %s<->%s\n", cor.getFirstRecord(), cor.getSecondRecord()));
			}
			
			return toRemove;
		}
		
	}
	
	protected String formatPath(List<Correspondence<MatchableTableColumn, Matchable>> path, BetweennessCentrality<MatchableTableColumn, Correspondence<MatchableTableColumn, Matchable>> between) {	
		StringBuilder sb = new StringBuilder();
		
		for(Correspondence<MatchableTableColumn, Matchable> edge : path) {
			sb.append(String.format("%s<-%.4f->%s\t", edge.getFirstRecord(), between.getEdgeRankScore(edge), edge.getSecondRecord()));
		}
		
		return sb.toString();
	}
	
	protected static class Conflict implements Comparable<Conflict> {
		public String header1;
		public String header2;
		public double value;
		public Conflict(String header1, String header2, double value) {
			this.header1 = header1;
			this.header2 = header2;
			this.value = value;
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		@Override
		public int compareTo(Conflict o) {
			return Double.compare(value, o.value);
		}
	}
	
	protected Pair<MatchableTableColumn, MatchableTableColumn> findConflict(Collection<MatchableTableColumn> cluster, DisjointHeaders dh) {
		
		Map<String, Collection<MatchableTableColumn>> groups = Q.group(cluster, new MatchableTableColumn.ColumnHeaderProjection());
		
		PriorityQueue<Conflict> conflicts = new PriorityQueue<>();
		List<String> headers = new ArrayList<>(groups.keySet());
		
		for(int i = 0; i < headers.size(); i++) {
			String header1 = headers.get(i);
			for(int j = i+1; j < headers.size(); j++) {
				String header2 = headers.get(j);
				
				// do not create a conflict if both headers are equal
				if(header1!=null && header2!=null && !header1.equals(header2) && dh.getDisjointHeaders(header1).contains(header2)) {
					Conflict c = new Conflict(header1, header2, groups.get(header1).size() * groups.get(header2).size());
					conflicts.add(c);
				}
			}
		}
		
		Conflict c = conflicts.poll();
		
		if(c==null) {
			return findConflictByTable(cluster);
		} else {
			return new Pair<MatchableTableColumn, MatchableTableColumn>(Q.firstOrDefault(groups.get(c.header1)), Q.firstOrDefault(groups.get(c.header2)));
		}
	}
	
	protected Pair<MatchableTableColumn, MatchableTableColumn> findConflictByTable(Collection<MatchableTableColumn> cluster) {
		Map<Integer, MatchableTableColumn> tableToColumn = new HashMap<>();

		for(MatchableTableColumn c : cluster) {
			
			MatchableTableColumn conflict = tableToColumn.get(c.getTableId());
			
			if(conflict==null) {
				tableToColumn.put(c.getTableId(), c);
			} else {
				return new Pair<MatchableTableColumn, MatchableTableColumn>(conflict, c);
			}
			
		}
		
		return null;
	}
}

