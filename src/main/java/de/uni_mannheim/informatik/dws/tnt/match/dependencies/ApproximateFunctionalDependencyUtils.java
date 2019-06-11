package de.uni_mannheim.informatik.dws.tnt.match.dependencies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.Table.ConflictHandling;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

public class ApproximateFunctionalDependencyUtils {

	/**
	 * calculates the violations and approximation rate for the approximate FD that is specified by determinant and dependant
	 * @param t		the table
	 * @param determinant	the expected determinant (the approximate FD will be calculated for this determinant)
	 * @param dependant		the expected dependant (the approximate FD will be calculated on all data from t for the columns in the union of determinant and dependant)
	 * @return
	 * @throws Exception
	 */
	public static ApproximateFunctionalDependency determineApproximateFunctionalDependency(Table t, Collection<TableColumn> determinant, TableColumn dependant) throws Exception {
		Collection<TableColumn> columns = Q.union(determinant, Q.toSet(dependant));
		Table fdOnly = t.project(columns);

		// do not deduplicate the table
		// - can reduce the number of conflicts (as duplicates are only counted once), which leads to an incorrect approximation rate
		// - approximation rate measures how many rows need to be removed for the FD to hold

		// if we deduplicate here, we measure how many value combinations violate the FD
		// if we do not deduplicate, we measure how many tuples violate the FD
		// fdOnly.deduplicate(fdOnly.getColumns());
		
		// TableColumn foreignKey = null;
		// for(TableColumn col : fdOnly.getColumns()) {
		// 	if("FK".equals(col.getHeader())) {
		// 		foreignKey = col;
		// 		break;
		// 	}
		// }
		
		// removeNULLsFromTable(fdOnly, foreignKey);
		
		Map<Integer, Integer> translation = t.projectColumnIndices(columns);
		
		// find the violations that prevent the relation from being supported by the FD
		
		// project the FD columns
		Collection<TableColumn> projectedColumns = null;
		// try {
			projectedColumns = Q.project(determinant, (c)->fdOnly.getSchema().get(translation.get(c.getColumnIndex())));
		// } catch(Exception e) {
		// 	e.printStackTrace();
		// }
		
		// find violations of the expected FD
		Collection<Pair<TableRow, TableRow>> conflicts = fdOnly.deduplicate(projectedColumns, ConflictHandling.ReturnConflicts, false);
		
		double approximationRate = (fdOnly.getSize()-conflicts.size())/(double)fdOnly.getSize();
		// double approximationRate = (t.getSize()-conflicts.size())/(double)t.getSize();
		// calculate the approrimation rate based on the size of the original table
		// double approximationRate = (t.getSize()-conflicts.size())/(double)t.getSize();
		ApproximateFunctionalDependency fd = new ApproximateFunctionalDependency(determinant, dependant, conflicts, approximationRate);
		
		return fd;
	}
	
	/**
	 * calculates the violations and approximation rate for all approximate FDs that are specified by determinant and one of the dependants
	 * @param t				the table
	 * @param determinant	the expected determinant
	 * @param dependant		a collection of dependants for which to check the expected determinant (one by one)
	 * @return	An approximate FD for every TableColumn in dependant
	 * @throws Exception
	 */
	public static Collection<ApproximateFunctionalDependency> determineApproximateFunctionalDependency(Table t, Collection<TableColumn> determinant, Collection<TableColumn> dependant) throws Exception {
		// test all combinations of columns as FD candidates
		return new ParallelProcessableCollection<>(dependant).map(
		// return new ProcessableCollection<>(dependant).map(
				(TableColumn expectedDependant, DataIterator<ApproximateFunctionalDependency> collector) 
				-> {
					ApproximateFunctionalDependency fd;
					try {
						fd = determineApproximateFunctionalDependency(t, determinant, expectedDependant);
						collector.next(fd);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}).get();
	}
	
	
	public static void removeNULLsFromTable(Table t, TableColumn foreignKey) {
		// remove rows without FK value or only NULL values except FK value
		List<TableRow> rowsToRemove = new LinkedList<>();
		for(TableRow r : t.getRows()) {
			
			boolean allNull = true;
			boolean fkNull = false;
			
			for(TableColumn c : t.getColumns()) {
				if(c!=foreignKey && r.get(c.getColumnIndex())!=null) {
					allNull = false;
				} else if(c==foreignKey && r.get(c.getColumnIndex())==null) {
					fkNull = true;
				}
			}
			
			if(allNull || fkNull) {
				rowsToRemove.add(r);
			}
			
		}
		
		Collections.sort(rowsToRemove, (r1,r2)->-Integer.compare(r1.getRowNumber(), r2.getRowNumber()));
		
		for(TableRow toRemove : rowsToRemove) {
			t.removeRow(toRemove.getRowNumber());
		}
		
		t.reorganiseRowNumbers();
	}
	
}