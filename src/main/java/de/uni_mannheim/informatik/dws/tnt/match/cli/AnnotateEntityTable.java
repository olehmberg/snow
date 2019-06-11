package de.uni_mannheim.informatik.dws.tnt.match.cli;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.data.DBpediaLookupServiceClient;
import de.uni_mannheim.informatik.dws.tnt.match.data.DBpediaLookupServiceResult;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.parallel.Parallel;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.JsonTableParser;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;

public class AnnotateEntityTable extends Executable {

	@Parameter(names="-table", required=true)
	private String tableLocation;

	@Parameter(names="-check")
	private boolean runInCheckMode = false;
	
	@Parameter(names = "-schemaOnly")
	private boolean schemaOnly = false;
	
	public static void main(String[] args) throws IOException {
		AnnotateEntityTable app = new AnnotateEntityTable();
		
		if(app.parseCommandLine(AnnotateEntityTable.class, args)) {
			app.run();			
		}
	}

	private final String NEW_ENTITY = "n";
	private final String WRONG_CLASS = "w";
	private final String UNCERTAIN = "?";
	
	public void run() throws IOException {
		Scanner s = new Scanner(System.in);
		Parallel.setReportIfStuck(false);
		
		// load the table
		System.out.println(String.format("Loading table '%s'", tableLocation));
		JsonTableParser p = new JsonTableParser();
		Table t = p.parseTable(new File(tableLocation));
		
		System.out.println(StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ", "));
		
		// check if annotation exist
		Map<Integer, String> mapping = new HashMap<>();
		Pair<String,Double>[] cors = t.getMapping().getMappedInstances();
		if(cors!=null) {
			for(int i = 0; i<cors.length; i++) {
				if(cors[i]!=null) {
					mapping.put(i, cors[i].getFirst());
				}
			}
		}
		
		System.out.println(String.format("%d/%d rows already annotated", mapping.size(), t.getRows().size()));
		if(mapping.size()>0) {
			Distribution<String> dist = Distribution.fromCollection(Q.where(mapping.values(), (s0)->!s0.startsWith("http")));
			System.out.println(dist.formatCompact());
		}
		
		// get class name or ask user
		String className = null;
		
		if(t.getMapping().getMappedClass()!=null && !t.getMapping().getMappedClass().getFirst().startsWith("e")) {
			className = t.getMapping().getMappedClass().getFirst();
		} else {
			System.out.print("Class: ");
			className = s.next();
			t.getMapping().setMappedClass(new Pair<>(className, 1.0));
		}
		System.out.println(String.format("Mapped to class '%s'", className));
		
		DBpediaLookupServiceClient lookup = new DBpediaLookupServiceClient();
		
		// get the rdfs:label columns
		TableColumn label = null;
		for(TableColumn c : t.getColumns()) {
			if("rdf-schema#label".equals(c.getHeader())) {
				label = c;
				t.getMapping().setMappedProperty(label.getColumnIndex(), new Pair<>("rdf-schema#label", 1.0));
				break;
			}
		}
		
		for(TableColumn c : t.getColumns()) {
			if(!"pk".equals(c.getHeader()) && c!=label) {
				Pair<String, Double> m = t.getMapping().getMappedProperty(c.getColumnIndex());
				if(m==null || m.getFirst()==null) {
					System.out.print(String.format("Property mapping for '%s': ", c.getHeader()));
					String input = s.nextLine();
					if(!"-".equals(input)) {
						m = new Pair<>(input,1.0);
						t.getMapping().setMappedProperty(c.getColumnIndex(), m);
						System.out.println(String.format("Column '%s' mapped to '%s'", c.getHeader(), m.getFirst()));
					} else {
						System.out.println(String.format("Column '%s' not mapped", c.getHeader()));
					}
					
				}
			}
		}
		
		while(label==null) {
			System.out.print("Label column name: ");
			String labelName = s.nextLine();
			
			if("-".equals(labelName)) {
				schemaOnly = true;
				break;
			}
			
			for(TableColumn c : t.getColumns()) {
				if(labelName.equals(c.getHeader())) {
					label = c;
					c.setHeader("rdf-schema#label");
					break;
				}
			}	
		}

		System.out.println(String.format("Label column is '%s'", label.toString()));
		
		if(!schemaOnly) {
//			if(label==null) {
//				System.out.println("No rdfs:label column!");
//			} else {
				int count = 0;
				// iterate over all rows
				for(TableRow r : t.getRows()) {
					if(!mapping.containsKey(r.getRowNumber()) || runInCheckMode) {
						System.out.println();
						System.out.println();
						if(runInCheckMode) {
							System.out.println(String.format("%d/%d rows checked", count, t.getRows().size()));
						} else {
							System.out.println(String.format("%d/%d rows annotated", mapping.size(), t.getRows().size()));
						}
						
						boolean quit = annotate(r, mapping, className, label, lookup, s, true);
						if(quit) {
							s.close();
							return;
						}
					}
					count++;
				}
				
				System.out.println("*** All rows annotated ***");
				
				// save table once all rows are annotated
				saveTable(t, mapping);
//			}
		} else {
//			if(label!=null) {
				String cls = className;
				if(!cls.endsWith(".csv")) {
					cls = cls + ".csv";
				}
				
				Pair<String, Double>[] schemaMapping = t.getMapping().getMappedProperties();
				if(schemaMapping!=null) {
					int lblIdx = label==null ? -1 : label.getColumnIndex();
					for(int i = 0; i < schemaMapping.length; i++) {
						if(schemaMapping[i]!=null) {
							if(schemaMapping[i].getFirst()==null) {
								t.getMapping().setMappedProperty(i, null);
							} else {
								if("rdf-schema#label".equals(schemaMapping[i].getFirst()) || "rdfs:label".equals(schemaMapping[i].getFirst()) || i==lblIdx) {
									t.getMapping().setMappedProperty(i, new Pair<>(String.format("%s::%s", cls, "http://www.w3.org/2000/01/rdf-schema#label"),1.0));
								} else if(!schemaMapping[i].getFirst().contains(cls)) {
									t.getMapping().setMappedProperty(i, new Pair<>(String.format("%s::%s", cls, schemaMapping[i].getFirst()),1.0));
								}							
							}
						}
					}
				} else if(label!=null) {
					t.getMapping().setMappedProperty(label.getColumnIndex(), new Pair<>(String.format("%s::%s", cls, "http://www.w3.org/2000/01/rdf-schema#label"),1.0));
				}

//			} else {
//				System.out.println("No label column");
//			}
			
			saveTable(t, null);
		}
		
		s.close();
	}
	
	protected boolean annotate(TableRow r, Map<Integer, String> mapping, String className, TableColumn label, DBpediaLookupServiceClient lookup, Scanner s, boolean skipExistingMappings) throws IOException {
		// get label
		Object labelValue = r.get(label.getColumnIndex());
		
		if(labelValue==null) {
			labelValue = "** NO LABEL **";
		}

		String annotation = mapping.get(r.getRowNumber());
		
		if(labelValue!=null || (runInCheckMode && Q.toSet(NEW_ENTITY, WRONG_CLASS, UNCERTAIN).contains(annotation))) {		
			// lookup possible entities
			String labelString = labelValue.toString();
			
			if(annotation!=null) {
				System.out.println(String.format("*** Annotation: '%s'", annotation));
			}
			
			DBpediaLookupServiceResult[] results = lookup.keywordSearch(labelString, className);
		
			if((results==null || results.length==0) && skipExistingMappings && !runInCheckMode) {
				annotation = NEW_ENTITY;
			}
		
			if(!skipExistingMappings) {
				annotation = null;
			}
			
			while(annotation==null) {
				System.out.println();
				
				System.out.println(r.format(50));
				
				int i = 0;
				for(DBpediaLookupServiceResult result : results) {
					String lbl = result.getLabel()==null ? "" : result.getLabel();
					String description = result.getDescription()==null ? "" : result.getDescription().substring(0,Math.min(100, result.getDescription().length()));
					System.out.println(String.format("[%d] %s\t%s", i++, lbl, description));
				}
				System.out.println("[more n] print complete description of lookup result n");
				// allow user to save table
				System.out.println("[s] save table");
				System.out.println("[w] wrong class");
				System.out.println("[n] new entity");
				System.out.println("[?] uncertain");
				System.out.println("[u] update another entry");
				System.out.println("[q] quit (without saving)");
				if(runInCheckMode) {
					System.out.println("[+] keep current assignment");
				}
				
				// ask user for entity
				System.out.print("> ");
				annotation = s.next();
				
				if("+".equals(annotation) && runInCheckMode) {
					annotation = mapping.get(r.getRowNumber());
				} else if("s".equals(annotation)) {
					saveTable(r.getTable(), mapping);
					annotation=null;
				} else if("w".equals(annotation)) {
					annotation = WRONG_CLASS;
				} else if("?".equals(annotation)) {
					annotation = UNCERTAIN;
				} else if("q".equals(annotation)) { 
					return true;
				} else if("u".equals(annotation)) {
					if(updateMapping(r.getTable(), mapping, className, label, lookup, s)) {
						return true;
					}
					annotation = null;
				} else if("n".equals(annotation)) {
					annotation = NEW_ENTITY;
				} else if("more".equals(annotation)) {
					annotation = s.next();
					int number = Integer.parseInt(annotation);
					
					if(number < results.length) {
						System.out.println(results[number].getUri());
						System.out.println(results[number].getDescription()); 
					} else {
						System.out.println(String.format("Invalid option [%d]", number));
					}
					annotation = null;
				} else {
					try {
						int number = Integer.parseInt(annotation);
						
						if(number < results.length) {
							annotation = results[number].getUri();
						} else {
							System.out.println(String.format("Invalid option [%d]", number));
							annotation = null;
						}
					} catch(Exception e) { 
					}
					
					// in any other case, just use the entered string as annotation
				}
			
			}
			
			if(annotation.contains("/page/")) {
				annotation = annotation.replace("/page/", "/resource/");
			}
			
			System.out.println(String.format("Selected '%s'", annotation));
			mapping.put(r.getRowNumber(), annotation);
		}
		
		return false;
	}
	
	protected boolean updateMapping(Table t, Map<Integer, String> mapping, String className, TableColumn label, DBpediaLookupServiceClient lookup, Scanner s) throws IOException {
		TableColumn pkCol = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"pk".equals(c.getHeader())));
		
		if(pkCol==null) {
			System.out.println("*** no PK column, cannot update mappings! ***");
		} else {
			System.out.print("PK value: ");
			String pk = s.next();
			TableRow selectedRow = null;
			for(TableRow r : t.getRows()) {
				String rowPK = (String)r.get(pkCol.getColumnIndex());
				if(pk.equals(rowPK)) {
					selectedRow = r;
					break;
				}
			}
			
			if(selectedRow==null) {
				System.out.println(String.format("*** PK %s does not exist! ***", pk));
			} else {
				return annotate(selectedRow, mapping, className, label, lookup, s, false);
			}
		}
		
		return false;
	}
	
	protected void saveTable(Table t, Map<Integer, String> mapping) throws IOException {
		
		if(mapping!=null) {
			for(Integer i : mapping.keySet()) {
				t.getMapping().setMappedInstance(i, new Pair<>(mapping.get(i), 1.0));
			}
		}
		
		JsonTableWriter w = new JsonTableWriter();
		w.setWriteMapping(true);
		w.write(t, new File(tableLocation));
		System.out.println("*** Table saved ***");
	}
}
