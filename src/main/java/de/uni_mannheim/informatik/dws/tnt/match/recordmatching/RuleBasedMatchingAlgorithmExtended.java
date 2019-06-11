/*
 * Copyright (c) 2017 Data and Web Science Group, University of Mannheim, Germany (http://dws.informatik.uni-mannheim.de/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package de.uni_mannheim.informatik.dws.tnt.match.recordmatching;

import java.time.Duration;
import java.time.LocalDateTime;

import org.apache.commons.lang3.time.DurationFormatUtils;

import de.uni_mannheim.informatik.dws.winter.matching.algorithms.MatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.Blocker;
import de.uni_mannheim.informatik.dws.winter.matching.rules.MatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * 
 * Implementation of rule-based matching. Applicable for identity resolution and schema matching using matching rules.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class RuleBasedMatchingAlgorithmExtended<RecordType extends Matchable, SchemaElementType extends Matchable, BlockedType extends Matchable, CorrespondenceType extends Matchable> implements MatchingAlgorithm<BlockedType, CorrespondenceType> {

	private DataSet<RecordType, SchemaElementType> dataset1;
	private DataSet<RecordType, SchemaElementType> dataset2;
	private Processable<Correspondence<CorrespondenceType, Matchable>> correspondences;
	private MatchingRule<BlockedType, CorrespondenceType> rule;
	private Blocker<RecordType, SchemaElementType, BlockedType, CorrespondenceType> blocker;
	private Processable<Correspondence<BlockedType, CorrespondenceType>> result;
	private String taskName = "Matching";
	/**
	 * @param dataset1
	 * 				the first dataset
	 * @param dataset2
	 * 				the second dataset
	 * @param correspondences
	 * 				correspondences between the two datasets
	 * @param rule
	 * 				the matching rule
	 * @param blocker
	 * 				the blocker
	 */
	public RuleBasedMatchingAlgorithmExtended(DataSet<RecordType, SchemaElementType> dataset1,
			DataSet<RecordType, SchemaElementType> dataset2,
			Processable<Correspondence<CorrespondenceType, Matchable>> correspondences,
			MatchingRule<BlockedType, CorrespondenceType> rule, 
			Blocker<RecordType, SchemaElementType, BlockedType, CorrespondenceType> blocker) {
		super();
		this.dataset1 = dataset1;
		this.dataset2 = dataset2;
		this.correspondences = correspondences;
		this.rule = rule;
		this.blocker = blocker;
	}
	public DataSet<RecordType, SchemaElementType> getDataset1() {
		return dataset1;
	}
	public DataSet<RecordType, SchemaElementType> getDataset2() {
		return dataset2;
	}
	public Processable<Correspondence<CorrespondenceType, Matchable>> getCorrespondences() {
		return correspondences;
	}
	public MatchingRule<BlockedType, CorrespondenceType> getRule() {
		return rule;
	}
	public Blocker<RecordType, SchemaElementType, BlockedType, CorrespondenceType> getBlocker() {
		return blocker;
	}
	public String getTaskName() {
		return taskName;
	}
	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}
	@Override
	public Processable<Correspondence<BlockedType, CorrespondenceType>> getResult() {
		return result;
	}
	
	public Processable<Correspondence<BlockedType, CorrespondenceType>> runBlocking(DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2, Processable<Correspondence<CorrespondenceType, Matchable>> correspondences) {
		return blocker.runBlocking(getDataset1(), getDataset2(), getCorrespondences());
	}
	
	public double getReductionRatio() {
		return getBlocker().getReductionRatio();
	}
	
	public void run() {
		LocalDateTime start = LocalDateTime.now();

		System.out.println(String.format("[%s] Starting %s",
				start.toString(), getTaskName()));

		System.out.println(String.format("Blocking %,d x %,d elements", getDataset1().size(), getDataset2().size()));
		
		// use the blocker to generate pairs
		Processable<Correspondence<BlockedType, CorrespondenceType>> allPairs = runBlocking(getDataset1(), getDataset2(), getCorrespondences());
		
		System.out
				.println(String
						.format("Matching %,d x %,d elements; %,d blocked pairs (reduction ratio: %s)",
								getDataset1().size(), getDataset2().size(),
								allPairs.size(), Double.toString(getReductionRatio())));

		// compare the pairs using the matching rule
		Processable<Correspondence<BlockedType, CorrespondenceType>> result = allPairs.map(rule);
		
		// report total matching time
		LocalDateTime end = LocalDateTime.now();
		
		System.out.println(String.format(
				"[%s] %s finished after %s; found %,d correspondences.",
				end.toString(), getTaskName(),
				DurationFormatUtils.formatDurationHMS(Duration.between(start, end).toMillis()), result.size()));
		
		this.result = result;
	}
}
