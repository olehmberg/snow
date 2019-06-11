package de.uni_mannheim.informatik.dws.tnt.match.cli;

import java.io.File;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SetAggregator;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

public class LearnEntityMatchersFromT2D extends Executable {

	@Parameter(names = "-web", required=true)
	private String webLocation;
	
	@Parameter(names = "-entityDefinition", required=true)
	private String entityDefinitionLocation;
	
	@Parameter(names = "-class", required=true)
	private String classLocation;
	
	@Parameter(names = "-schema", required=true)
	private String schemaLocation;
	
	@Parameter(names = "-instance", required=true)
	private String instanceLocation;
	
	public static void main(String[] args) throws Exception {
		LearnEntityMatchersFromT2D app = new LearnEntityMatchersFromT2D();
		
		if(app.parseCommandLine(LearnEntityMatchersFromT2D.class, args)) {
			app.run();
		}
	}
	
	public void run() throws Exception {
		WebTables web = WebTables.loadWebTables(new File(webLocation), true, true, true, false);
		KnowledgeBase kb = KnowledgeBase.loadKnowledgeBase(new File(new File(entityDefinitionLocation), "tables/"), null, null, true, true);
		kb.loadCandidateKeys(new File(new File(entityDefinitionLocation), "keys/"));
		
		MatchingGoldStandard classMapping = new MatchingGoldStandard();
		classMapping.loadFromCSVFile(new File(classLocation));
		
		MatchingGoldStandard schema = new MatchingGoldStandard();
		schema.loadFromCSVFile(new File(schemaLocation));
		
		MatchingGoldStandard instance = new MatchingGoldStandard();
		instance.loadFromCSVFile(new File(instanceLocation));
		

		Collection<Table> usedTables = addClassMapping(web, kb, classMapping);
		
		addInstanceMapping(usedTables, instance);
		
		usedTables = addSchemaMapping(usedTables, schema, kb);
		
		// group tables by class
		
		// create union table per class
		
		// deduplicate union table?
		
		// learn matching rule
	}
	
	private Collection<Table> addClassMapping(WebTables web, KnowledgeBase kb, MatchingGoldStandard classMapping) {
		// list existing entity definitions
		Set<String> entityDefinitions = kb.getClassIds().keySet();
		System.out.println(String.format("Loaded %d entity definitions", entityDefinitions.size()));
		
		// create class map
		Map<String, String> classMap = Pair.toMap(classMapping.getPositiveExamples());
		System.out.println(String.format("Annotations contain %d classes", classMap.size()));
		
		Collection<Table> usedTables = new LinkedList<>();
		Distribution<String> classDistribution = new Distribution<>();
		
		// create class mapping from gold standard
		for(Table t : web.getTables().values()) {
			
			String cls = classMap.get(t.getPath());
			
			if(cls!=null && entityDefinitions.contains(cls)) {
				t.getMapping().setMappedClass(new Pair<>(cls, 1.0));
				usedTables.add(t);
				classDistribution.add(cls);
			}
			
		}
		
		System.out.println(String.format("%d tables match loaded entity definitions: %s", usedTables.size(), classDistribution.formatCompact()));
		return usedTables;
	}
	
	private void addInstanceMapping(Collection<Table> tables, MatchingGoldStandard instance) {
		// create instance map
		Map<String, String> instanceMap = Pair.toMap(instance.getPositiveExamples());
		
		// create instance mapping from gold standard
		for(Table t : tables) {
			for(TableRow r : t.getRows()) {
				String instanceUri = instanceMap.get(r.getIdentifier());
				
				if(instanceUri!=null) {
					t.getMapping().setMappedInstance(r.getRowNumber(), new Pair<>(instanceUri, 1.0));
				}
			}
		}
	}
	
	private Collection<Table> addSchemaMapping(Collection<Table> tables, MatchingGoldStandard schema, KnowledgeBase kb) throws Exception {

		Collection<Table> results = new LinkedList<>();
		Distribution<String> classDistribution = new Distribution<>();
		Distribution<String> instanceDistribution = new Distribution<>();
		
		// list required properties for entity definitions
		Processable<Pair<Integer, Set<String>>> entityProperties = kb.getSchema()
			.aggregate(
				(MatchableTableColumn record,DataIterator<Pair<Integer, String>> resultCollector) 
					-> {
						String propUri = record.getIdentifier().split("::")[1];
						if(!"URI".equals(propUri)) {
							resultCollector.next(new Pair<>(record.getTableId(), propUri));
						}
					}
				, new SetAggregator<>());
		Map<Integer, Set<String>> entityPropertyMap = Pair.toMap(entityProperties.get());
		
		// create schema map
		Map<String, String> schemaMap = Pair.toMap(schema.getPositiveExamples());
		
		// create schema mapping from gold standard
		for(Table t : tables) {
			
			Collection<TableColumn> usedColumns = new LinkedList<>();
			
			int entityTypeId = kb.getClassIds().get(t.getMapping().getMappedClass().getFirst());
			Set<String> usedProperties = entityPropertyMap.get(entityTypeId);
			
			if(usedProperties!=null) {
				for(TableColumn c : t.getColumns()) {
					String prop = schemaMap.get(c.getIdentifier());
					
					if(prop!=null && usedProperties.contains(prop)) {
						t.getMapping().setMappedProperty(c.getColumnIndex(), new Pair<>(prop,1.0));
						usedColumns.add(c);
					}
				}
				
				if(usedProperties.size()==usedColumns.size()) {
					// project used columns
					Table used = t.project(usedColumns);
					used.getMapping().setMappedClass(t.getMapping().getMappedClass());
					results.add(used);
					classDistribution.add(t.getMapping().getMappedClass().getFirst());
					
					// projection does not update the table mapping!
					Map<Integer, Integer> columnProjection = t.projectColumnIndices(usedColumns);
					for(Integer iOld : columnProjection.keySet()) {
						Integer iNew = columnProjection.get(iOld);
						
						used.getMapping().setMappedProperty(iNew, t.getMapping().getMappedProperty(iOld));
					}
					for(TableRow r : used.getRows()) {
						Pair<String, Double> mapping  = t.getMapping().getMappedInstance(r.getRowNumber());
						if(mapping!=null) {
							used.getMapping().setMappedInstance(r.getRowNumber(), mapping);
							instanceDistribution.add(t.getMapping().getMappedClass().getFirst());
						}
					}
				}
			}
		}
		
		System.out.println(String.format("%d tables match attributes of loaded entity definitions: %s", results.size(), classDistribution.formatCompact()));
		System.out.println(String.format("%d instance correspondences: %s", instanceDistribution.getPopulationSize(), instanceDistribution.formatCompact()));
		return results;
	}
}
