package de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * 
 * Creates blocking keys from the values of matched determinants
 * 
 * @author Oliver
 *
 */
public class DeterminantRecordBlockingKeyGenerator extends BlockingKeyGenerator<MatchableTableRow, MatchableTableDeterminant, MatchableTableRow> {

	private static final long serialVersionUID = 1L;

	@Override
	public void generateBlockingKeys(MatchableTableRow record,
			Processable<Correspondence<MatchableTableDeterminant, Matchable>> correspondences,
			DataIterator<Pair<String, MatchableTableRow>> resultCollector) {

		if(correspondences!=null) {

			// get a set of all determinants that have matches (and will hence be used to generate a blocking key)
			// the same determinant can be matched to many tables, which will lead to replication of the blocking key values if we use all correspondences
			Set<MatchableTableDeterminant> determinants = new HashSet<>();
			for(Correspondence<MatchableTableDeterminant, Matchable> cor : correspondences.get()) {
				
				MatchableTableDeterminant det = null;
				
				if(cor.getFirstRecord().getTableId()==record.getTableId()) {
					det = cor.getFirstRecord();
				} else {
					det = cor.getSecondRecord();
				}
				
				determinants.add(det);
			}
			
			// generate a blocking key for every matched determinant
			for(MatchableTableDeterminant det : determinants) {

				// get the record's values for the determinant's attributes
				Object[] values = record.get(Q.toPrimitiveIntArray(Q.project(det.getColumns(), new MatchableTableColumn.ColumnIndexProjection())));
				
				// sort the values to make sure that they match, even if the attributes in the different records have a different order
				List<String> sorted = Q.sort(Q.toString(Arrays.asList(values)));
				
				// create the blocking key
				String blockingKey = StringUtils.join(sorted, "/");
				
//				System.out.println(String.format("{%d} %s\t%s", record.getDataSourceIdentifier(), record.getIdentifier(), blockingKey));
				
				// create the result
				resultCollector.next(new Pair<String, MatchableTableRow>(blockingKey, record));
				
			}
		}
	}

}
