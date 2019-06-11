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
package de.uni_mannheim.informatik.dws.tnt.match.dependencies;

import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.data.StitchedModel;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.metanome.FDTreeWrapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ForeignKeyFDFilter {

    public void filter(StitchedModel model) {
        
        for(Table t : model.getTables().values()) {

            FDTreeWrapper fds = new FDTreeWrapper(t.getColumns());

            TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));

            // remove FDs without FK in the determinant
            for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(StitchedFunctionalDependencyUtils.allClosures(t.getSchema().getFunctionalDependencies(), model))) {
                if(fd.getFirst().contains(fk) || fd.getFirst().size()==0) {
                    fds.addMinimalFunctionalDependency(fd);
                }
            }

            fds.minimise();
            
            t.getSchema().setFunctionalDependencies(fds.getFunctionalDependencies());
        }

    }

    
}