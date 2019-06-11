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
package de.uni_mannheim.informatik.dws.snow;

import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.*;
import java.util.*;
import org.apache.commons.lang.StringUtils;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SnowLog {
    
	public static void printStep(String message) {
		System.out.println("*********************************************************************");
		System.out.println("*********************************************************************");
		System.out.println(String.format("***************** %s",message));
		System.out.println("*********************************************************************");
		System.out.println("*********************************************************************");
	}

    public static void printCandidateKeys(Table t, boolean onlyIncludingForeignKey) {
		TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));
		if(fk!=null) {
			for(Collection<TableColumn> key : t.getSchema().getCandidateKeys()) {
				if(key.contains(fk)) {
					System.out.println(String.format("\t{%s}", 
						StringUtils.join(Q.project(key, new TableColumn.ColumnHeaderProjection()), ",")
						));
				}
			}
		}
	}

	public static void printFunctionalDependencies(Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies) {
		System.out.println("*** Functional Dependencies");
		for(Collection<TableColumn> det : functionalDependencies.keySet()) {
			Collection<TableColumn> dep = functionalDependencies.get(det);
			System.out.println(String.format("\t{%s} -> {%s}", 
					StringUtils.join(Q.project(det, new TableColumn.ColumnHeaderProjection()), ","),
					StringUtils.join(Q.project(dep, new TableColumn.ColumnHeaderProjection()), ",")
					));
		}
	}

	public static void printFunctionalDependenciesWithoutKeys(Table t) {
		System.out.println("*** Functional Dependencies");
		for(Collection<TableColumn> det : t.getSchema().getFunctionalDependencies().keySet()) {
			Collection<TableColumn> dep = t.getSchema().getFunctionalDependencies().get(det);
			System.out.println(String.format("\t{%s} -> {%s}", 
					StringUtils.join(Q.project(det, new TableColumn.ColumnHeaderProjection()), ","),
					StringUtils.join(Q.project(dep, new TableColumn.ColumnHeaderProjection()), ",")
					));
		}
	}

	public static void printFunctionalDependencies(Table t) {
		System.out.println("*** Functional Dependencies");
		for(Collection<TableColumn> det : t.getSchema().getFunctionalDependencies().keySet()) {
			Collection<TableColumn> dep = t.getSchema().getFunctionalDependencies().get(det);
			System.out.println(String.format("\t{%s} -> {%s}", 
					StringUtils.join(Q.project(det, new TableColumn.ColumnHeaderProjection()), ","),
					StringUtils.join(Q.project(dep, new TableColumn.ColumnHeaderProjection()), ",")
					));
		}
		System.out.println("*** Candidate Keys");
		for(Collection<TableColumn> key : t.getSchema().getCandidateKeys()) {
			System.out.println(String.format("\t{%s}", 
					StringUtils.join(Q.project(key, new TableColumn.ColumnHeaderProjection()), ",")
					));
		}
	}
}