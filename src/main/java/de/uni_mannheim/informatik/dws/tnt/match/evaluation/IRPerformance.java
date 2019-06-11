/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.dws.tnt.match.evaluation;

import de.uni_mannheim.informatik.dws.winter.model.Performance;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class IRPerformance extends Performance {

	private double precision;
	private double recall;
	
	public IRPerformance(double p, double r) {
		super(0,0,0);
		precision=p;
		recall=r;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Performance#getPrecision()
	 */
	@Override
	public double getPrecision() {
		return precision;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Performance#getRecall()
	 */
	@Override
	public double getRecall() {
		return recall;
	}
	
	
}
