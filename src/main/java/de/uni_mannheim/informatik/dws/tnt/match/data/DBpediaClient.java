package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;

import com.google.gson.Gson;

public class DBpediaClient {

	public static class QueryResults {
		SparqlResult results;
	}
	
	public static class SparqlResult {
		ResultBinding[] bindings;
	}
	
	public static class ResultBinding {
		Value property;
	}
	
	public static class Value {
		String value;
	}
	
	public static String getAbstract(String resourceUri) throws UnsupportedEncodingException {
		
		HttpClient client = new HttpClient();

//		select+distinct+%3Fabstract+where+%7B%3Chttp%3A%2F%2Fdbpedia.org%2Fresource%2FLagoinha_Church%3E+dbo%3Aabstract+%3Fabstract%7D+LIMIT+100&format=text%2Fhtml&CXML_redir_for_subjs=121&CXML_redir_for_hrefs=&timeout=30000&debug=on&run=+Run+Query+

		String query = String.format("" +
                "select ?property where {\n" +
                "  <%s> <http://dbpedia.org/ontology/abstract> ?property\n" +
                "  filter langMatches(lang(?property),\"en\")\n" +
                "}", resourceUri);
				
	    String request = String.format("http://dbpedia.org/sparql?default-graph-uri=%s&query=%s", URLEncoder.encode("http://dbpedia.org", "utf-8"), URLEncoder.encode(query, "utf-8"));
	    HttpMethod method = new GetMethod(request);
	    method.setRequestHeader("Accept", "application/json");

	    QueryResults results = null;
	    
	    String resultValue = null;
	    
	    try {

//	    	System.out.println(request);
	    	client.executeMethod(method);

	    	InputStream ins = method.getResponseBodyAsStream();

	    	Gson g = new Gson();
	    	
	    	results = g.fromJson(new InputStreamReader(ins), QueryResults.class);
	    	
	    	if(results!=null && results.results.bindings.length>0) {
	    		resultValue = results.results.bindings[0].property.value;
	    	}
	    	

	    } catch (HttpException he) {

	      System.err.println("Http error connecting to dbpedia.org");

	    } catch (IOException ioe) {

	      System.err.println("Unable to connect to dbpedia.org");

	    }

	    method.releaseConnection();
	    
	    return resultValue;
	    
	}
	
}
