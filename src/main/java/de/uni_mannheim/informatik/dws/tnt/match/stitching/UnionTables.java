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
package de.uni_mannheim.informatik.dws.tnt.match.stitching;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TableSchemaStatistics;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableContext;

/**
 * 
 * Creates union tables from a collection of tables.
 * All tables with the same schema are merged into the same union table
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class UnionTables {

	// maps a schema to a representative table for that schema
	HashMap<String, Table> schemaToTable = new HashMap<>();

	ContextAttributeExtractor context = new ContextAttributeExtractor();

	public Collection<Collection<Table>> getTableClustersFromURLs(Collection<Table> tables, double minSupport, boolean verbose) throws URISyntaxException {

		// split the tables by the length of their URL
		Map<Integer, Collection<Table>> groupedByLength = Q.group(tables, 
			(t)->{
				try {
					return context.getUriFragmentValues(t).size();
				} catch(Exception e) {
					e.printStackTrace();
					return 0;
				}
			});

		if(verbose) {
			System.out.println(String.format("Found %d groups of URLs with different length",groupedByLength.size()));
			// for(Integer length : groupedByLength.keySet()) {
			// 	System.out.println(String.format("\tGroup of length %d",length));
			// 	for(Table t : Q.take(groupedByLength.get(length), 10)) {
			// 		System.out.println(String.format("\t\t%s",t.getContext().getUrl()));
			// 	}
			// }
		}

		Collection<Collection<Table>> clustering = new LinkedList<>();

		for(Integer length : groupedByLength.keySet()) {
			Collection<Table> tablesToCluster = groupedByLength.get(length);
			if(verbose) {
				System.out.println(String.format("Group of length %d",length));
				for(Table t : Q.take(groupedByLength.get(length), 10)) {
					System.out.println(String.format("\t%s",t.getContext().getUrl()));
				}
			}

			// get all urls
			Map<String, Collection<Table>> urlToTable = Q.group(tablesToCluster, (t)->t.getContext().getUrl());
			Set<String> urls = new HashSet<>(urlToTable.keySet());

			// calculate support for each value/URL-Index combination
			Map<Pair<String, Integer>, Set<String>> valueAndIndexToURL = new HashMap<>();
			for(String url : urls) {
				List<String> uriValues = context.getUriFragmentValues(url);
				for(int i = 0; i < uriValues.size(); i++) {
					Set<String> urlsWithValueAtIndex = MapUtils.get(valueAndIndexToURL, new Pair<>(uriValues.get(i), i), new HashSet<>());
					urlsWithValueAtIndex.add(url);
				}
			}

			// choose the item with the highest support count
			Pair<String, Integer> maxValueAndIndex = Q.<Pair<String, Integer>,Integer>max(valueAndIndexToURL.keySet(), (key)->valueAndIndexToURL.get(key).size());
			double maxSupport = 0.0;

			if(maxValueAndIndex!=null) {
				double maxSupportCount = valueAndIndexToURL.get(maxValueAndIndex).size();
				maxSupport = maxSupportCount / (double)urls.size();
			}

			Set<Table> tablesInClusters = new HashSet<>();
			Set<String> URLsInClusters = new HashSet<>();

			if(maxSupport >= minSupport) {

				System.out.println(String.format("Choosing ('%s', uri %d) for clustering",maxValueAndIndex.getFirst(), maxValueAndIndex.getSecond()));

				// repeat the selection procedure: pick the max-frequent item, create clusters for it on all positions, choose the next most-frequent item, do the same, etc.
				while(valueAndIndexToURL.size()>0) {
					Map<Pair<String, Integer>, Set<String>> selectedItems = new HashMap<>(valueAndIndexToURL);

					// get the next-most frequent item at the selected position
					Pair<String, Integer> mostFrequentItemAtSelectedIndex = Q.<Pair<String, Integer>,Integer>max(
						Q.where(
							valueAndIndexToURL.keySet(), 
							(p)->p.getSecond().intValue()==maxValueAndIndex.getSecond().intValue()), 
						(key)->Q.without(valueAndIndexToURL.get(key), URLsInClusters).size());		// don't count the URLs which have already been assigned to a cluster

					String mostFrequentValue = null;
					if(
						// there is still an item left
						mostFrequentItemAtSelectedIndex!=null 
						&& 
						// and not all of its URLs have been assigned to clusters already
						Q.without(valueAndIndexToURL.get(mostFrequentItemAtSelectedIndex), URLsInClusters).size()>0
					) {
						mostFrequentValue = mostFrequentItemAtSelectedIndex.getFirst();
					} else {
						break;
					}

					System.out.println(String.format("\tSelected value '%s' for clustering",mostFrequentValue));

					// choose the url part (value) for clustering
					for(Pair<String, Integer> item : new HashSet<>(selectedItems.keySet())) {
						// keep only frequent items which are either at the same position or have the same value
						if(
							!item.getFirst().equals(mostFrequentValue)
						) {
								selectedItems.remove(item);
						}
					}

					System.out.println(String.format("\tFound %d frequent items with value '%s'", selectedItems.size(), mostFrequentValue));

					// create the cluster for the max-frequent itemset first
					// then create clusters for the same value at other positions
					LinkedList<Pair<String, Integer>> sortedItems = new LinkedList<>();
					sortedItems.addAll(
						Q.sort(
							selectedItems.keySet(),
							(p1,p2)->-Integer.compare(selectedItems.get(p1).size(), selectedItems.get(p2).size())));			// sort them by support count descending

					// create the clusters
					
					while(sortedItems.size()>0) {
						// choose the item with the highest support count
						Pair<String, Integer> valueAndIndex = sortedItems.poll();

						// get all URLs supporting the item
						Set<String> urlsForItem = valueAndIndexToURL.get(valueAndIndex);
						Set<String> unclusteredURLsForItem = new HashSet<>(Q.without(urlsForItem, URLsInClusters));

						if(unclusteredURLsForItem.size()>0) {
							double initialSupportCount = urlsForItem.size();
							double supportCount = unclusteredURLsForItem.size();
							double initialSupport = initialSupportCount / (double)urls.size();
							double support = supportCount / (double)urls.size();

							if(verbose) {
								System.out.println(String.format("Frequent item ('%s', uri %d): support=%f (initial=%f)",
									valueAndIndex.getFirst(),
									valueAndIndex.getSecond(),
									support,
									initialSupport
								));
							}

							// create the cluster
							Collection<Table> cluster = new LinkedList<>();
							for(String url : unclusteredURLsForItem) {
								cluster.addAll(urlToTable.get(url));
								tablesInClusters.addAll(urlToTable.get(url));
							}
							clustering.add(cluster);
							URLsInClusters.addAll(urlsForItem);
						}
						valueAndIndexToURL.remove(valueAndIndex);
					}
				}
			}

			Collection<Table> unclusteredTables = Q.without(tablesToCluster, tablesInClusters);
			if(unclusteredTables.size()>0) {
				clustering.add(unclusteredTables);
			}
		}

		if(verbose) {
			System.out.println(String.format("Created %d clusters",clustering.size()));
			int clusterId = 0;
			for(Collection<Table> cluster : clustering) {
				System.out.println(String.format("\tCluster %d: %d tables",clusterId++, cluster.size()));
			}
		}

		return clustering;
	}

	public Map<String, Integer> generateContextAttributes(Collection<Table> tables, boolean fromUriFragments, boolean fromUriQuery) throws URISyntaxException {
    	/***********************************************
    	 * Context Columns
    	 ***********************************************/
    	// first iterate over all tables and collect the possible context attributes then add all these attributes to all tables
    	// needed to make sure all tables have the same schema w.r.t. the added columns
		
		// maps the added context attributes to their relative column index
		Map<String, Integer> contextAttributes = new HashMap<>();
		
		contextAttributes.put(ContextColumns.PAGE_TITLE_COLUMN, 0);
		contextAttributes.put(ContextColumns.TALBE_HEADING_COLUMN, 1);
		
    	List<String> fragmentAttributes = new LinkedList<>();
    	List<String> queryAttributes = new LinkedList<>();
    	for(Table t : tables) {
    		for(String s : context.getUriFragmentParts(t)) {
    			if(!fragmentAttributes.contains(s)) {
    				fragmentAttributes.add(s);
    			}
    		}
    		for(String s : context.getUriQueryParts(t)) {
    			if(!queryAttributes.contains(s)) {
    				queryAttributes.add(s);
    			}
    		}
    	}

    	int colIdx = contextAttributes.size();
    	
    	if(fromUriFragments) {
	    	for(int i=0; i<fragmentAttributes.size(); i++) {
	    		contextAttributes.put(fragmentAttributes.get(i), colIdx++);
	    	}
    	}
    	if(fromUriQuery) {
	    	for(int i=0; i<queryAttributes.size(); i++) {
	    		contextAttributes.put(queryAttributes.get(i), colIdx++);
	    	}
    	}
    	
    	return contextAttributes;
	}
	
	public Collection<Table> create(List<Table> tables, Map<String, Integer> contextAttributes) {
	
    	/***********************************************
    	 * Create Union Tables (Schema)
    	 ***********************************************/
    	// the table schema statistics
    	TableSchemaStatistics stat = new TableSchemaStatistics();
    	
    	// sort the tables by file name to get reproducible table names for the output
    	Collections.sort(tables, new Comparator<Table>() {

			@Override
			public int compare(Table o1, Table o2) {
				return o1.getPath().compareTo(o2.getPath());
			}});
    	
    	// iterate over all tables and calculate schema statistics
    	for(Table t : tables) {
    		
    		// add the table
    		stat.addTable(t);
    		
    		// generate the schema (ordered set of all column headers)
    		String schema = getTableSchema(t);
    		
    		// add a representative table for that schema, if none exists yet
    		if(!schemaToTable.containsKey(schema)) {

    			try {
					createUnionTable(t, contextAttributes);
				} catch (Exception e) {
					e.printStackTrace();
				}
    	
    		}
    	}

    	// print the schema statistics to the console
    	stat.printSchemaList();
		
    	/***********************************************
    	 * Create Union Tables (Data)
    	 ***********************************************/
    	
    	Processable<Table> allTables = new ParallelProcessableCollection<>(tables);
    	
    	allTables.foreach(new DataIterator<Table>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void next(Table t) {
				// generate the schema (ordered set of all column headers)
	    		String schema = getTableSchema(t);
				
				// get the table that represents t's schema (and which will be the union table)
				Table union = schemaToTable.get(schema);
				
				if(union!=null) {
				
					synchronized (union) {
					
						// add provenance information to the columns
						for(TableColumn c : union.getColumns()) {
							int extraColumns = contextAttributes.size();
							
							if(!SpecialColumns.isSpecialColumn(c) && !ContextColumns.isContextColumn(c)) {
								TableColumn c2 = t.getSchema().get(c.getColumnIndex() - extraColumns);
								c.addProvenanceForColumn(c2);
							}
						}
						
						try {
							context.addUnionColumns(t, true, contextAttributes);
							
							union.append(t);
						} catch (URISyntaxException e) {
							e.printStackTrace();
						}
				
					}
					
				}
			}
			
			@Override
			public void initialise() {
			}
			
			@Override
			public void finalise() {
			}
		});
    	
    	return schemaToTable.values();
	}

	public Collection<Collection<Table>> getStitchableTables(Collection<Table> tables) {
		Map<String, Collection<Table>> schemaToTable = new HashMap<>();
		for(Table t : tables) {
			String schema = getTableSchema(t);
			MapUtils.getFast(schemaToTable, schema, (s)-> new HashSet<>()).add(t);
		}
		return schemaToTable.values();
	}

	private String getTableSchema(Table t) {
		// generate the schema (ordered set of all column headers)
		String schema = String.format("%s", TableSchemaStatistics.generateSchemaString(t));
		return schema;
	}
	
	private Table createUnionTable(Table t, Map<String, Integer> contextAttributes) throws Exception {
		// add a copy of the table as new union table
		Table union = t.project(t.getColumns());
		union.setTableId(schemaToTable.size());
		union.setPath(Integer.toString(union.getTableId()));
		
		// add context information
		TableContext ctx = new TableContext();
		ctx.setTableNum(t.getContext().getTableNum());
		URL url = new URL(t.getContext().getUrl());
		ctx.setUrl(String.format("%s://%s", url.getProtocol(), url.getHost()));
		union.setContext(ctx);
		
		// remove all rows from the union table
		union.clear();
		
		// reset  the provenance information for the table
		for(TableColumn c : t.getColumns()) { 
			if(!SpecialColumns.isSpecialColumn(c)) {
				TableColumn uc = union.getSchema().get(c.getColumnIndex());
				List<String> prov = new LinkedList<String>();
				uc.setProvenance(prov);
				prov = new LinkedList<String>();
				c.setProvenance(prov);
			}
		}
		
		// add the context attributes
		context.addUnionColumns(union, false, contextAttributes);
		
		String schema = getTableSchema(t);
		
		schemaToTable.put(schema, union);
		
		return union;
	}
	
}
