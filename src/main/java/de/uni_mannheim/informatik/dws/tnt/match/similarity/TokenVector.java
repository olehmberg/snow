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
package de.uni_mannheim.informatik.dws.tnt.match.similarity;

import java.util.*;
import org.apache.commons.lang.StringUtils;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TokenVector<TokenType> {

    private Map<TokenType, Double> vector;

    public TokenVector() {
        vector = new HashMap<>();
    }

    public Double get(TokenType token) {
        return vector.get(token);
    }

    public void set(TokenType token, Double value) {
        vector.put(token, value);
    }

    public Set<TokenType> getDimensions() {
        return vector.keySet();
    }

    public Map<TokenType, Double> toMap() {
        return vector;
    }

    public String format() {
        List<String> lst = new LinkedList<>();
        for(Map.Entry<TokenType,Double> e : vector.entrySet()) {
            lst.add(String.format("%s: %f", e.getKey().toString(), e.getValue()));
        }
        return StringUtils.join(lst, ", ");
    }
}