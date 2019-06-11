package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import au.com.bytecode.opencsv.CSVReader;
import de.uni_mannheim.informatik.dws.t2k.index.dbpedia.DBpediaIndexer;
import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.lod.LodTableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.LodCsvTableParser;


/**
 * 
 * Model of a knowledge base.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class KnowledgeBase implements Serializable {

	private static final long serialVersionUID = 1L;
	
	// data that will be matched: records and schema
	private DataSet<MatchableTableRow, MatchableTableColumn> records = new ParallelHashedDataSet<>();
	private DataSet<MatchableTableColumn, MatchableTableColumn> schema = new ParallelHashedDataSet<>();
	private DataSet<MatchableTable, MatchableTableColumn> tables = new ParallelHashedDataSet<>();
	private DataSet<MatchableTableDeterminant, MatchableTableColumn> candidateKeys = new ParallelHashedDataSet<>();
	private Processable<Correspondence<MatchableTableDeterminant, Matchable>> inclusionDependencies = new ParallelProcessableCollection<>();
	private DataSet<MatchableTableCell, MatchableTableColumn> labels = new ParallelHashedDataSet<>();
	
	// translation for DBpedia property URIs: URI string to integer
	private LinkedList<String> properties = new LinkedList<>();
	private HashMap<String, Integer> propertyIds = new HashMap<>();
	
	private Map<Integer, LocalDateTime[]> dateRanges = new HashMap<>();
	
	// translation from table file name to table id
	private Map<String, Integer> tableIds = new HashMap<>();
	
	//translation from table id to DBpedia class
	private Map<Integer, String> classIndices = new HashMap<>();
	
	// translation from class name to table id
	private Map<String, Integer> classToId = new HashMap<>();
	
	// rdfs:label
	private MatchableLodColumn rdfsLabel;
	
	// lookup for tables by id
	private HashMap<Integer, Integer> sizePerTable = new HashMap<Integer, Integer>();
	
	// class weights
	private HashMap<Integer, Double> classWeight = new HashMap<Integer, Double>();
	
	//class hierarchy
	private static HashMap<String, String> classHierarchy = new HashMap<String, String>();
	
	// candidate keys: table id -> list of keys; key = list of property names
	private Map<String, Collection<Collection<String>>> candidateKeyDefinitions;
	
	public static final String RDFS_LABEL = "rdf-schema#label";

	private static boolean doSerialise = true;
	public static void setDoSerialise(boolean serialise) {
		doSerialise = serialise;
	}
	
	public static KnowledgeBase loadKnowledgeBase(File location, IIndex index, SurfaceForms sf, boolean convertTypes, boolean parseLists) throws FileNotFoundException {
		// look for serialised version
		File ser;

		if(sf==null) {
			ser = new File(location.getParentFile(), location.getName() + ".bin");
		} else {
			ser = new File(location.getParentFile(), location.getName() + "_surfaceforms.bin");
		}
		
		// index from serialised version is not implemented, so only load serialised if we did not get an index to fill
    	if(index==null) {
    		if(ser.exists()) {
				return KnowledgeBase.deserialise(ser);
    		} else if(location.getName().endsWith(".bin")) {
    			return KnowledgeBase.deserialise(location); 
    		}
    	}
    	
    	// load KB from location
    	KnowledgeBase kb = new KnowledgeBase();		
		kb.load(location, index, sf, convertTypes, parseLists);

		// serialise
		if(doSerialise) {
			kb.serialise(ser);
		}
		
		return kb;
	}
	
	public void load(File location, IIndex index, SurfaceForms sForms, boolean convertTypes, boolean parseLists) {
    	/***********************************************
    	 * Load DBpedia
    	 ***********************************************/

    	LodCsvTableParser lodParser = new LodCsvTableParser();
    	lodParser.setConvertValues(convertTypes);
    	lodParser.setParseLists(parseLists);
    	DBpediaIndexer indexer = new DBpediaIndexer();
    	
    	List<File> dbpFiles = null;
    	
    	if(location.isDirectory()) {
			dbpFiles = Arrays.asList(location.listFiles());
    	} else {
    		dbpFiles = Arrays.asList(new File[] { location});
    	}
    	
    	int tblIdx = 0;
    	for(File f : dbpFiles) {
			System.out.println("Loading Knowledge Base Table " + f.getName());
			Table tDBp = lodParser.parseTable(f);
			tDBp.setTableId(tblIdx);
			String className = tDBp.getPath().replace(".csv", "").replace(".gz", "");
			
			MatchableTable mt = new MatchableTable(tblIdx, className);
			tables.add(mt);
			tableIds.put(className, tblIdx);			
			
			if(tDBp.getSchema().getSize()>1 && "rdf-schema#label".equals(tDBp.getSchema().get(1).getHeader())) {
				
				tDBp.setSubjectColumnIndex(1);
				
			}
	    		
				if(dbpFiles.size()==1) {
					for(TableColumn tc : tDBp.getSchema().getRecords()) {
						System.out.println(String.format("{%s} [%d] %s (%s): %s", tDBp.getPath(), tc.getColumnIndex(), tc.getHeader(), tc.getDataType(), tc.getUri()));
					}
				}
	    		
				LodTableColumn[] cols = tDBp.getColumns().toArray(new LodTableColumn[tDBp.getSchema().getSize()]);
				
				// assign the class to the range of the rdfs:label property
				for(LodTableColumn c : cols) {
					if("rdf-schema#label".equals(c.getHeader())) {
						c.setRange(className);
					}
				}
				
				// assign the range of the original property to the label column
	    		// remove object properties and keep only "_label" columns (otherwise we will have duplicate property URLs)
	    		
	    		List<TableColumn> removedColumns = new LinkedList<>();
	    		for(LodTableColumn tc : cols) {
	    			if(tc.isReferenceLabel()) {
	    				Iterator<TableColumn> it = tDBp.getSchema().getRecords().iterator();
	    				
	    				while(it.hasNext()) {
	    					LodTableColumn ltc = (LodTableColumn)it.next();
	    					
	    					if(!ltc.isReferenceLabel() && ltc.getUri().equals(tc.getUri())) {
	    						tc.setRange(ltc.getRange());
	    						// it.remove();
								// removedColumns.add(ltc.getColumnIndex());
								removedColumns.add(ltc);
	    					}
	    				}
	    			}
				}
				
				// remove the columns
				for(TableColumn c : removedColumns) {
					tDBp.removeColumn(c);
				}
	    		
	    		// re-create value arrays
	    		// for(TableRow r : tDBp.getRows()) {
	    		// 	Object[] values = new Object[tDBp.getSchema().getSize()];
	    			
	    		// 	int newIndex = 0;
	    		// 	for(int i=0; i < r.getValueArray().length; i++) {
	    		// 		if(!removedColumns.contains(i)) {
	    		// 			values[newIndex++] = r.getValueArray()[i];
	    		// 		}
	    		// 	}
	    			
	    		// 	r.set(values);
	    		// }
	    		
	    		// create the schema
	    		MatchableTableColumn[] matchableColumns = new MatchableLodColumn[tDBp.getSchema().getSize()];
				int colIdx=0;
				MatchableTableColumn rdfsLabelColumn = null;
	    		for(TableColumn tc : tDBp.getSchema().getRecords()) {
	    			
	    			MatchableLodColumn mc = new MatchableLodColumn(tblIdx, tc);
	    			schema.add(mc);
					matchableColumns[colIdx++] = mc;
					
					if("rdf-schema#label".equals(mc.getHeader())) {
						rdfsLabelColumn=mc;
					}
	    		}
	    		
	    		// create the records
		    	for(TableRow r : tDBp.getRows()) {
		    		// make sure only the instance with the most specific class remains in the final dataset for each URI
		    		MatchableTableRow mr = records.getRecord(r.getIdentifier());
		    		
		    		if(mr==null) {
						// mr = new MatchableTableRow(r, tblIdx, matchableColumns);
						mr = new MatchableLodRow(r, tblIdx, matchableColumns);
		    		} else {
		    			String clsOfPrevoisRecord = classIndices.get(mr.getTableId());
		    			String clsOfCurrentRecord = tDBp.getPath().replace(".csv", "").replace(".gz", "");
		    			
		    			if(classHierarchy.get(clsOfPrevoisRecord)==null){
		    				continue;
		    			}else {
		    				String cls;
		    				boolean flag = false;
		    				while((cls = classHierarchy.get(clsOfPrevoisRecord)) != null){
		    					if(cls.equals(clsOfCurrentRecord)){
		    						flag = true;
		    						break;
		    					}else{
		    						clsOfPrevoisRecord = cls;
		    					}
		    				}
		    				if(flag == false){
//		    					mr = new MatchableTableRow(r, tblIdx, matchableColumns);
		    					mr = new MatchableLodRow(r, tblIdx, matchableColumns);
		    				}
		    			}
		    				
		    		}
		    		
//		    		MatchableLodRow mr = new MatchableLodRow(r, tblIdx, matchableColumns);
					
					if(sForms!=null && rdfsLabelColumn!=null) {
						// add all surface forms to the rdfs:label property
						Set<String> names = new HashSet<>();
						if(mr.get(rdfsLabelColumn.getColumnIndex()) instanceof String[]) {
							names.addAll(Arrays.asList((String[])mr.get(rdfsLabelColumn.getColumnIndex())));
						} else {
							names.add((String)mr.get(rdfsLabelColumn.getColumnIndex()));
						}

						Set<String> surfaceForms = new HashSet<>();
						for(String name : names) {
							if(name!=null) {
								surfaceForms.addAll(sForms.getSurfaceForms(name.toLowerCase()));
							}
						}

						surfaceForms.addAll(names);

						if(surfaceForms.size()>1) {
							mr.set(rdfsLabelColumn.getColumnIndex(), surfaceForms.toArray(new String[surfaceForms.size()]));
						}
					}

					if(rdfsLabelColumn!=null) {
						labels.add(new MatchableTableCell(mr, rdfsLabelColumn));
					}

		    		records.add(mr);
		    	}
		    	sizePerTable.put(tblIdx, tDBp.getSize());
		    	
		    	
		    	classIndices.put(tblIdx, className);
		    	classToId.put(className, tblIdx);
		    	
		    	// we don't need the table rows anymore (MatchableTableRow is used from now on)
		    	tDBp.clear();
		    	tblIdx++;
//			} else {
//				System.out.println(" -> no key!");
//			}
    	}
    	
    	// add classes from the class hierarchy which have not been loaded (but can be mapped via the hierarchy)
    	for(String cls : new HashSet<>(classToId.keySet())) {
    		
    		String superClass = classHierarchy.get(cls);
    		
    		while(superClass!=null) {
    			
    			if(!classToId.containsKey(superClass)) {
    				// create a URI column for the class
    				MatchableLodColumn mc = new MatchableLodColumn(
    						superClass + ".csv", 
    						"URI", 
    						"http://www.w3.org/2002/07/owl#Thing", 
    						tblIdx, 
    						0,
    						"URI", 
    						DataType.link);
	    			schema.add(mc);
    				
    				// create a label column for the class
    				mc = new MatchableLodColumn(
    						superClass + ".csv", 
    						"http://www.w3.org/2000/01/rdf-schema#label", 
    						"http://www.w3.org/2000/01/rdf-schema#Literal", 
    						tblIdx, 
    						1,
    						"rdf-schema#label", 
    						DataType.string);
	    			schema.add(mc);
    				
    				MatchableTable mt = new MatchableTable(tblIdx, superClass);
    				tables.add(mt);
    				classToId.put(superClass, tblIdx);
    				classIndices.put(tblIdx, superClass);
    				tblIdx++;
    			}
    			
    			superClass = classHierarchy.get(superClass);
    		}
    		
    	}
    	
    	LodCsvTableParser.endLoadData();
    	
//    	calculate class weights
    	calculateClassWeight();
    	
    	determineDateRanges();
    	
		if(index!=null) {
			System.out.println("Indexing ...");
			indexer.indexInstances(index, records.get(), classIndices, sForms);
		}
    	
    	System.out.println(String.format("%,d DBpedia Instances loaded from CSV", records.size()));
    	System.out.println(String.format("%,d DBpedia Properties / %,d Property IDs", schema.size(), propertyIds.size()));
    }
	
	public static void loadClassHierarchy(String location) throws IOException{
		System.out.println("Loading Class Hierarchy...");
		BufferedReader tsvReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(location))));
		String values;
		while((values = tsvReader.readLine()) != null){
			String[] cls = values.split("\t")[0].split("/");
			String[] superCls = values.split("\t")[1].split("/");
			classHierarchy.put(cls[cls.length-1].replaceAll("\"", ""), superCls[superCls.length-1].replaceAll("\"", ""));
		}
		tsvReader.close();
		System.out.println("Loaded Class Hierarchy for " + classHierarchy.size() + " Resources.");
	}
	
	public void loadCandidateKeys(File location) throws IOException {
		
		candidateKeyDefinitions = new HashMap<>();
		
		for(File f : location.listFiles()) {

			String className = f.getName().replace(".csv", "").replace(".gz", "");
			
			LinkedList<Collection<String>> keys = new LinkedList<>();
			
			CSVReader r = new CSVReader(new FileReader(f));
			
			String[] values = null;
			
			while((values = r.readNext())!=null) {
				
				keys.add(Arrays.asList(values));
				
			}
			
			r.close();
		
			candidateKeyDefinitions.put(className, keys);
		}
		
		for(String cls : candidateKeyDefinitions.keySet()) {
			Collection<Collection<String>> keys = candidateKeyDefinitions.get(cls);
			
			int classId = getClassIds().get(cls);
			
			Map<String, MatchableTableColumn> attributeToColumn = new HashMap<>();
			
			for(MatchableTableColumn c : getSchema().where((c)->c.getTableId()==classId).get()) {
				attributeToColumn.put(c.getHeader(), c);
			}
			
			for(Collection<String> key : keys) {
				Set<MatchableTableColumn> columns = new HashSet<>();
				
				for(String attribute : key) {
					columns.add(attributeToColumn.get(attribute));
				}
				
				MatchableTableDeterminant det = new MatchableTableDeterminant(classId, columns);
				candidateKeys.add(det);
			}
		}
	}
	
	public void loadInclusionDependencies(File location) throws IOException {
		
		BufferedReader r = new BufferedReader(new FileReader(location));
		
		String line = null;
		int lineNo = 1;
		
		while((line = r.readLine())!=null) {
			
			String[] values = line.split("\t");
			
			if(values!=null && values.length==5) {
				String cls1 = values[0];
				String[] attributes1 = values[1].split(",");
				String cls2 = values[2];
				String[] attributes2 = values[3].split(",");
				Double score = Double.parseDouble(values[4]);
				
				int cls1Id = getClassIds().get(cls1);
				Map<String, MatchableTableColumn> attributeToColumn1 = new HashMap<>();
				for(MatchableTableColumn c : getSchema().where((c)->c.getTableId()==cls1Id).get()) {
					attributeToColumn1.put(c.getHeader(), c);
				}
				int cls2Id = getClassIds().get(cls2);
				Map<String, MatchableTableColumn> attributeToColumn2 = new HashMap<>();
				for(MatchableTableColumn c : getSchema().where((c)->c.getTableId()==cls2Id).get()) {
					attributeToColumn2.put(c.getHeader(), c);
				}
				
				Set<MatchableTableColumn> cols1 = new HashSet<>();
				for(String attribute : attributes1) {
					cols1.add(attributeToColumn1.get(attribute));
				}
				
				Set<MatchableTableColumn> cols2 = new HashSet<>();
				for(String attribute : attributes2) {
					cols2.add(attributeToColumn2.get(attribute));
				}
				
				Correspondence<MatchableTableDeterminant, Matchable> id = new Correspondence<>(
						new MatchableTableDeterminant(cls1Id, cols1),
						new MatchableTableDeterminant(cls2Id, cols2),
						score
						);
				
				inclusionDependencies.add(id);
			} else {
				System.err.println(String.format("[KnowledgeBase] Malformed inclusion dependency in file %s line %d: %s", location.getAbsolutePath(), lineNo, line));
			}
			
			lineNo++;
			
		}
		
		r.close();
		
	}
	
	public static KnowledgeBase deserialise(File location) throws FileNotFoundException {
		System.out.println("Deserialising Knowledge Base");
		
        Kryo kryo = new Kryo();
        // kryo.setRegistrationRequired(false);
        Input input = new Input(new FileInputStream(location));
        KnowledgeBase kb = kryo.readObject(input, KnowledgeBase.class);
        input.close();
        
        return kb;
	}
	
	public void serialise(File location) throws FileNotFoundException {
		System.out.println("Serialising Knowledge Base");
		
		Kryo kryo = new Kryo();
		// kryo.setRegistrationRequired(false);
        Output output = new Output(new FileOutputStream(location));
        kryo.writeObject(output, this);
        output.close();
	}
    
    public void calculateClassWeight(){
		double max = -1;
        
	      for (Entry<Integer, Integer> tableSize : getTablesSize().entrySet()) {
	            if (tableSize.getValue() < 1) {
	                continue;
	            }
	            if (tableSize.getValue() > max) {
	                max = tableSize.getValue();
	            }
	        }      
	        
	        for(Entry<Integer, Integer> tableSize : getTablesSize().entrySet()){
	        	double value = 0;
	        	if (tableSize.getValue() < 1) {
	                value = 1;
	            }
	            value =tableSize.getValue()/max;
	            value = 1-value;
	            classWeight.put(tableSize.getKey(), value);
	            }
	        
		
	}
    
    public void determineDateRanges() {
    	for(MatchableTableRow row : records.get()) {
    		
    		for(MatchableTableColumn col : row.getSchema()) {
    			
    			if(col.getType()==DataType.date) {
    				
    				LocalDateTime[] range = MapUtils.get(dateRanges, col.getColumnIndex(), new LocalDateTime[2]);
    				
//    				Map<Integer, Integer> indexTranslation = getPropertyIndices().get(row.getTableId());
//    				if(indexTranslation==null) {
//    					System.err.println("Missing property index translation for table " + row.getTableId());
//    				}
    				
//    				'secondColumnIndex' ('globalId' of dbpedia property) is used to get 'columnIndex' of dbpedia property in a respective table
//    				Integer translatedIndex = indexTranslation.get(col.getColumnIndex());
//    				if(translatedIndex!=null) {

	    				Object obj = row.get(col.getColumnIndex());
	    				
	    				Object[] values = null;
	    				if(obj!=null && obj.getClass().isArray()) {
	    					values = (Object[])obj;
	    				} else {
	    					values = new Object[] { obj };
	    				}
	    				
	    				for(Object o : values) {
		    				if(o!=null && o instanceof LocalDateTime) {
		    				
		    					LocalDateTime value = (LocalDateTime)o;
		    					
		    					if(range[0]==null || value.compareTo(range[0]) < 0) {
		    						range[0] = value;
		    					}
		    					
		    					if(range[1]==null || value.compareTo(range[1]) > 0) {
		    						range[1] = value;
		    					}
		    					
		    				} else {
		    					if(o!=null && !(o instanceof LocalDateTime)) {
		    						System.err.println(String.format("{%s} row %d property %s has value of invalid type: '%s' (%s)", this.classIndices.get(row.getTableId()), row.getRowNumber(), col.getIdentifier(), obj, obj.getClass()));
		    					}
		    				}
	    				}
//    				}
    				
    			}
    			
    		}
    		
    	}
    }
    
	public DataSet<MatchableTableRow, MatchableTableColumn> getRecords() {
		return records;
	}

	public DataSet<MatchableTableColumn, MatchableTableColumn> getSchema() {
		return schema;
	}

	/**
	 * @return the labels
	 */
	public DataSet<MatchableTableCell, MatchableTableColumn> getLabels() {
		return labels;
	}

	public DataSet<MatchableTable, MatchableTableColumn> getTables() {
		return tables;
	}
	
	public LinkedList<String> getProperties() {
		return properties;
	}

	public HashMap<String, Integer> getPropertyIds() {
		return propertyIds;
	}

	public MatchableLodColumn getRdfsLabel() {
		return rdfsLabel;
	}
	
	public HashMap<Integer, Double> getClassWeight(){
		return classWeight;
	}
	/**
	 * @return the classIndices
	 */
	public Map<Integer, String> getClassIndices() {
		return classIndices;
	}
	/**
	 * @return the tableIds
	 */
	public Map<String, Integer> getClassIds() {
		return classToId;
	}
	
	/**
	 * @return the tables
	 */
	public HashMap<Integer, Integer> getTablesSize() {
		return sizePerTable;
	}
	
	/**
	 * @return the classHierarchy mapping class -> super class
	 */
	public static HashMap<String, String> getClassHierarchy() {
		return classHierarchy;
	}
	
	/**
	 * @return the candidateKeys
	 */
	public Map<String, Collection<Collection<String>>> getCandidateKeyDefinitions() {
		return candidateKeyDefinitions;
	}
	
	public DataSet<MatchableTableDeterminant, MatchableTableColumn> getCandidateKeys() {
		return candidateKeys;
	}
	
	/**
	 * @return the inclusionDependencies
	 */
	public Processable<Correspondence<MatchableTableDeterminant, Matchable>> getInclusionDependencies() {
		return inclusionDependencies;
	}
}
