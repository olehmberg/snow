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

import de.uni_mannheim.informatik.dws.winter.model.BigPerformance;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import org.apache.commons.lang.StringUtils;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SnowPerformance {

    private List<Pair<String, Performance>> performanceByStep = new LinkedList<>();
    private Map<String, Collection<Pair<String,Performance>>> detailedPerformanceByStep = new HashMap<>();

    public void addPerformance(String stepName, Performance p) {
        performanceByStep.add(new Pair<>(stepName, p));
    }

    public void addPerformanceDetail(String stepName, String detail, Performance performance) {
        MapUtils.get(detailedPerformanceByStep, stepName, new LinkedList<>()).add(new Pair<>(detail, performance));
    }

    public void print() {
        System.out.println("Performance log:");
        for(Pair<String, Performance> p : performanceByStep) {
            BigPerformance big = null;
            if(p.getSecond() instanceof BigPerformance) {
                big = (BigPerformance)p.getSecond();
                System.out.println(String.format("P: %.6f\tR: %.6f\tF1: %.6f\t\t \tW: %-15d\t%s", 
                    big.getPrecision(),
                    big.getRecall(),
                    big.getF1(),
                    big.getNumberOfCorrectTotalBig().toBigInteger(),
                    p.getFirst()
                ));
            } else {
                System.out.println(String.format("P: %.6f\tR: %.6f\tF1: %.6f\t\t \tW: %-15d\t%s", 
                    p.getSecond().getPrecision(),
                    p.getSecond().getRecall(),
                    p.getSecond().getF1(),
                    p.getSecond().getNumberOfCorrectTotal(),
                    p.getFirst()
                ));
            }

            Collection<Pair<String,Performance>> details = detailedPerformanceByStep.get(p.getFirst());
            if(details!=null) {
                for(Pair<String, Performance> d : details) {
                    if(d.getSecond() instanceof BigPerformance) {
                        BigPerformance b = (BigPerformance)d.getSecond();
                        System.out.println(String.format("|\tP: %.6f\tR: %.6f\tF1: %.6f\t|\tW: %-15d\tRel: %6.2f%%\t|\tS: %-15d\tRel: %6.2f%%\t|\t%s", 
                            b.getPrecision(),
                            b.getRecall(),
                            b.getF1(),
                            b.getNumberOfCorrectTotalBig().toBigInteger(),
                            // StringUtils.rightPad(Integer.toString(d.getSecond().getNumberOfCorrectTotal()), 15, " "),
                            b.getNumberOfCorrectTotalBig().setScale(10).divide(big.getNumberOfCorrectTotalBig(), RoundingMode.HALF_UP).multiply(new BigDecimal(100.0)),
                            b.getNumberOfPredicedBig().toBigInteger(),
                            b.getNumberOfPredicedBig().setScale(10).divide(big.getNumberOfPredicedBig(), RoundingMode.HALF_UP).multiply(new BigDecimal(100.0)),
                            d.getFirst()
                        ));
                    } else {
                        System.out.println(String.format("|\tP: %.6f\tR: %.6f\tF1: %.6f\t|\tW: %-15d\tRel: %6.2f%%\t|\tS: %-15d\tRel: %6.2f%%\t|\t%s", 
                            d.getSecond().getPrecision(),
                            d.getSecond().getRecall(),
                            d.getSecond().getF1(),
                            d.getSecond().getNumberOfCorrectTotal(),
                            // StringUtils.rightPad(Integer.toString(d.getSecond().getNumberOfCorrectTotal()), 15, " "),
                            d.getSecond().getNumberOfCorrectTotal() / (double)p.getSecond().getNumberOfCorrectTotal() * 100.0,
                            d.getSecond().getNumberOfPredicted(),
                            d.getSecond().getNumberOfPredicted() / (double)p.getSecond().getNumberOfPredicted() * 100.0,
                            d.getFirst()
                        ));
                    }
                }
            }  
        }  
    }
}