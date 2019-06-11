package de.uni_mannheim.informatik.dws.tnt.match.data;

import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class DBpediaSparqlClient {

	public static String getAbstract(String resourceUri) {
		try {
	        ParameterizedSparqlString qs = new ParameterizedSparqlString( "" +
	                "select ?abstract where {\n" +
	                "  ?resource <http://dbpedia.org/ontology/abstract> ?abstract\n" +
	                "  filter langMatches(lang(?abstract),\"en\")\n" +
	                "}" );
	
	        Resource resource = ResourceFactory.createResource(resourceUri);
	        qs.setParam( "resource", resource);
	
	        System.out.println( qs );
	
	        QueryExecution exec = QueryExecutionFactory.sparqlService( "http://dbpedia.org/sparql", qs.asQuery() );
  
	        ResultSet results = exec.execSelect();
	
	        String result = null;
	        
	        while ( results.hasNext() ) {
	            
	        	result = results.next().get( "abstract").asLiteral().getString();
	            
	        }
	        
	        return result;
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
}
