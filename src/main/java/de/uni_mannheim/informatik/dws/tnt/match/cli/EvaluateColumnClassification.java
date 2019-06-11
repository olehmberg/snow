package de.uni_mannheim.informatik.dws.tnt.match.cli;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

public class EvaluateColumnClassification extends Executable {

	@Parameter(names = "-result", required=true)
	private String resultLocation;
	
	@Parameter(names = "-reference", required=true)
	private String referenceLocation;
	
	@Parameter(names = "-weightByOriginalColumns")
	private boolean weightByOriginalColumns = false;

	@Parameter(names = "-weightByRows")
	private boolean weightByRows = false;
	
	@Parameter(names = "-globalLog")
	private String globalLogLocation;
	
	@Parameter(names = "-excludeIndependentContext")
	private boolean excludeIndependentContext = false;
	
	public static void main(String[] args) throws Exception {
		EvaluateColumnClassification app = new EvaluateColumnClassification();
		
		if(app.parseCommandLine(EvaluateColumnClassification.class, args)) {
			app.run();
		}
	}

	public void run() throws IOException {
		// load reference classification
		Map<String, Prediction> reference = readClassificationFile(new File(referenceLocation));
		
		// load classification results
		Map<String, Prediction> result = readClassificationFile(new File(resultLocation));
		
		// measure performance
		Map<String, Performance> perfByClass = new HashMap<>();
		Performance overallPerformance = new Performance(0, 0, 0);
		
		for(String colIdRef : Q.sort(reference.keySet())) {
			Prediction ref = reference.get(colIdRef);
			Prediction pred = result.get(colIdRef);
			
			String refClass = ref.className;
			String predClass = pred == null ? null : pred.className;
			
			boolean correct = refClass.equals(predClass);
			
			int refWeight = (weightByOriginalColumns || weightByRows) ? ref.weight : 1;
			
			int predWeight = 0;
			if(predClass!=null) {
				predWeight = (weightByOriginalColumns || weightByRows) ? pred.weight : 1;
			}
			
			Performance classPerfRef = MapUtils.get(perfByClass, refClass, new Performance(0, 0, 0));
			classPerfRef = new Performance(
					classPerfRef.getNumberOfCorrectlyPredicted() + (correct ? Math.min(predWeight, refWeight) : 0), 
					classPerfRef.getNumberOfPredicted() + (correct ? predWeight : 0), 
					classPerfRef.getNumberOfCorrectTotal() + refWeight);
			perfByClass.put(refClass, classPerfRef);
			
			if(!correct && predClass!=null) {
				System.out.println(String.format("Incorrect classification '%s'[%d] for '%s' (actually '%s'[%d])", predClass, predWeight, colIdRef, refClass, refWeight));
				
				Performance classPerfPred = MapUtils.get(perfByClass, predClass, new Performance(0, 0, 0));
				classPerfPred = new Performance(
						classPerfPred.getNumberOfCorrectlyPredicted(), 
						classPerfPred.getNumberOfPredicted() + predWeight, 
						classPerfPred.getNumberOfCorrectTotal());
				perfByClass.put(predClass, classPerfPred);
			}
			
			
			overallPerformance = new Performance(
					overallPerformance.getNumberOfCorrectlyPredicted() + (correct ? Math.min(predWeight, refWeight) : 0), 
					overallPerformance.getNumberOfPredicted() + (predClass!=null ? predWeight : 0), 
					overallPerformance.getNumberOfCorrectTotal() + refWeight);
			
			result.remove(colIdRef);
		}
		
		for(String colIdPred : result.keySet()) {
			Prediction pred = result.get(colIdPred);
			String predClass = pred.className;
			
			Performance classPerf = MapUtils.get(perfByClass, predClass, new Performance(0, 0, 0));
			
			int weight = (weightByOriginalColumns || weightByRows) ? pred.weight : 1;
			
			System.out.println(String.format("Classification of unknown column '%s' as '%s' [%d]", colIdPred, predClass, weight));

			classPerf = new Performance(
					classPerf.getNumberOfCorrectlyPredicted(), 
					classPerf.getNumberOfPredicted() + weight, 
					classPerf.getNumberOfCorrectTotal());
			perfByClass.put(predClass, classPerf);
			
			overallPerformance = new Performance(
					overallPerformance.getNumberOfCorrectlyPredicted(), 
					overallPerformance.getNumberOfPredicted() + weight, 
					overallPerformance.getNumberOfCorrectTotal());
		}
		
		System.out.println(String.format("Overall Performance (total weight: %d)", overallPerformance.getNumberOfCorrectTotal()));
		System.out.println(String.format("\tPrecision: %f\n\tRecall: %f\n\tF1: %f", overallPerformance.getPrecision(), overallPerformance.getRecall(), overallPerformance.getF1()));
		
		for(String cls : perfByClass.keySet()) {
			Performance clsPerformance = perfByClass.get(cls);
			System.out.println();
			System.out.println(String.format("Performance for '%s' (weight: %d)", cls, clsPerformance.getNumberOfCorrectlyPredicted()));
			System.out.println(String.format("\tPrecision: %f\n\tRecall: %f\n\tF1: %f", clsPerformance.getPrecision(), clsPerformance.getRecall(), clsPerformance.getF1()));
		}
		

		if(globalLogLocation!=null) {
			File tbls = new File(resultLocation);
			BufferedWriter w = new BufferedWriter(new FileWriter(globalLogLocation, true));
			w.write(String.format("%s\t%s\t%s\t%s\t%s\t%.6f\t%.6f\t%.6f\t%d\n", 
					tbls.getParentFile().getName(),
					tbls.getName(),
					"overall",
					(weightByOriginalColumns ? "micro columns" : (weightByRows ? "micro rows" : "macro" )),
					(excludeIndependentContext ? "_excl_ctx_ind" : ""),
					overallPerformance.getPrecision(),
					overallPerformance.getRecall(),
					overallPerformance.getF1(),
					overallPerformance.getNumberOfCorrectTotal()
					));
			
			for(String cls : perfByClass.keySet()) {
				Performance clsPerformance = perfByClass.get(cls);
				w.write(String.format("%s\t%s\t%s\t%s\t%s\t%.6f\t%.6f\t%.6f\t%d\n", 
						tbls.getParentFile().getName(),
						tbls.getName(),
						cls,
						(weightByOriginalColumns ? "micro columns" : (weightByRows ? "micro rows" : "macro" )),
						(excludeIndependentContext ? "_excl_ctx_ind" : ""),
						clsPerformance.getPrecision(),
						clsPerformance.getRecall(),
						clsPerformance.getF1(),
						clsPerformance.getNumberOfCorrectTotal()
						));
			}
			
			w.close();
		}
	}
	
	protected static class Prediction {
		public String columnId;
		public String columnHeader;
		public String className;
		public Integer weight;
		
		public Prediction(String columnId, String columnHeader, String className, Integer weight) {
			this.columnId = columnId;
			this.columnHeader = columnHeader;
			this.className = className;
			this.weight = weight;
		}
	}
	
	protected Map<String, Prediction> readClassificationFile(File f) throws IOException {
		BufferedReader r = new BufferedReader(new FileReader(f));
		
		Map<String, Prediction> classification = new HashMap<>();
		
		String line = null;
		
		while((line=r.readLine())!=null) {
			String[] parts = line.split("\t");
			int weight = weightByOriginalColumns ? Integer.parseInt(parts[3]) : Integer.parseInt(parts[4]); 
//			classification.put(parts[0], new Pair<>(parts[1], weight));
			
			if(!excludeIndependentContext || !(ContextColumns.isContextColumn(parts[2]) && "independent".equals(parts[1]))) {
				classification.put(parts[0], new Prediction(parts[0], parts[2], parts[1], weight));
			}
		}
		
		r.close();
		
		return classification;
	}
	
	
}
