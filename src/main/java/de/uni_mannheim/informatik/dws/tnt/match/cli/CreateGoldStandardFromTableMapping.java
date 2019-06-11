package de.uni_mannheim.informatik.dws.tnt.match.cli;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.JsonTableParser;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;
import edu.stanford.nlp.util.StringUtils;

public class CreateGoldStandardFromTableMapping extends Executable {

	@Parameter(names = "-web", required=true) 
	private String webLocation;
	
	@Parameter(names = "-keepMappedRowsOnly")
	private boolean keepMappedRowsOnly = false;
	
	@Parameter(names = "-eval", required=true)
	private String evalLocation;
	
	public static void main(String[] args) throws IOException {
		CreateGoldStandardFromTableMapping app = new CreateGoldStandardFromTableMapping();
		
		if(app.parseCommandLine(CreateGoldStandardFromTableMapping.class, args)) {
			app.run();
		}
	}

	public void run() throws IOException {
		
		JsonTableParser p = new JsonTableParser();
		Table t = p.parseTable(new File(webLocation));
		
		if(keepMappedRowsOnly) {
			ArrayList<TableRow> rows = new ArrayList<>();
			
			for(TableRow r : t.getRows()) {
				if(t.getMapping().getMappedInstance(r.getRowNumber())!=null) {
					rows.add(r);
				}
			}
			t.setRows(rows);
			t.reorganiseRowNumbers();
		}
		
		System.out.println(StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ","));
		
		Scanner s = new Scanner(System.in);
		for(TableColumn c : t.getColumns()) {
			if(!"pk".equals(c.getHeader())) {
				Pair<String, Double> m = t.getMapping().getMappedProperty(c.getColumnIndex());
				if(m==null || m.getFirst()==null) {
					System.out.print(String.format("Property mapping for '%s': ", c.getHeader()));
					String input = s.nextLine();
					m = new Pair<>(input,1.0);
					t.getMapping().setMappedProperty(c.getColumnIndex(), m);
				}
				System.out.println(String.format("Column '%s' mapped to '%s'", c.getHeader(), m.getFirst()));
			}
		}
		s.close();
		
		// write the table with mapping
		JsonTableWriter tw = new JsonTableWriter();
		tw.setWriteMapping(true);
		tw.write(t, new File(evalLocation, t.getPath()));
		
		// write the row correspondences
		BufferedWriter w = new BufferedWriter(new FileWriter(new File(evalLocation, t.getPath() + "_goldstandard.csv")));
		for(TableRow r : t.getRows()) {
			Pair<String, Double> mapping = t.getMapping().getMappedInstance(r.getRowNumber());
			if(mapping!=null && mapping.getFirst().startsWith("http")) {
				w.write(String.format("%s\t%s\ttrue\n", r.getIdentifier(), String.format("%s.csv:%s", t.getMapping().getMappedClass().getFirst(), mapping.getFirst())));
			}
		}
		w.close();
	}
}
