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

public class DBpediaLookupServiceClient {

	private String uri;
	
	public DBpediaLookupServiceClient() {
		uri = "http://lookup.dbpedia.org/api/search.asmx";
	}
	
	public DBpediaLookupServiceClient(String uri) {
		this.uri = uri;
	}
	
	public DBpediaLookupServiceResult[] keywordSearch(String query, String className) throws UnsupportedEncodingException {
	    HttpClient client = new HttpClient();

	    String request = String.format("%s/KeywordSearch?QueryClass=%s&QueryString=%s", uri, className, URLEncoder.encode(query, "utf-8"));
	    HttpMethod method = new GetMethod(request);
	    method.setRequestHeader("Accept", "application/json");

	    DBpediaLookupServiceResults results = null;
	    
	    try {

//	    	System.out.println(request);
	    	client.executeMethod(method);

	    	InputStream ins = method.getResponseBodyAsStream();

	    	Gson g = new Gson();
	    	results = g.fromJson(new InputStreamReader(ins), DBpediaLookupServiceResults.class);
	    	
//	      BufferedReader r = new BufferedReader(new InputStreamReader(ins));
//	      String line = null;
//	      while((line = r.readLine())!=null) {
//	    	  System.out.println(line);
//	      }

	    } catch (HttpException he) {

	      System.err.println("Http error connecting to lookup.dbpedia.org");

	    } catch (IOException ioe) {

	      System.err.println("Unable to connect to lookup.dbpedia.org");

	    }

	    method.releaseConnection();
	    
	    if(results!=null) {
	    	return results.getResults();
	    } else {
	    	return null;
	    }
	}
	
}
