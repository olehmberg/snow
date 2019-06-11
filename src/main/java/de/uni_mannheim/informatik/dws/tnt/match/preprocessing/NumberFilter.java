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
package de.uni_mannheim.informatik.dws.tnt.match.preprocessing;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * Detects and removes date and time values from strings
 * 
  * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class NumberFilter extends Executable {

    final static  String Digits = "(\\p{Digit}+)";
    final static  String HexDigits = "(\\p{XDigit}+)";
    // an exponent is 'e' or 'E' followed by an optionally
    // signed decimal integer.
    final static  String Exp = "[eE][+-]?" + Digits;
    final static Pattern numberPattern = Pattern.compile(("[\\x00-\\x20]*" + // Optional leading
                                               // "whitespace"
            "[+-]?(" + // Optional sign character
            "NaN|" + // "NaN" string
            "Infinity|" + // "Infinity" string

            // A decimal floating-point string representing a finite
            // positive
            // number without a leading sign has at most five basic pieces:
            // Digits . Digits ExponentPart FloatTypeSuffix
            //
            // Since this method allows integer-only strings as input
            // in addition to strings of floating-point literals, the
            // two sub-patterns below are simplifications of the grammar
            // productions from the Java Language Specification, 2nd
            // edition, section 3.10.2.

            // Digits ._opt Digits_opt ExponentPart_opt FloatTypeSuffix_opt
            // "(((" + Digits + "(\\.)?(" + Digits + "?)(" + Exp + ")?)|" +
            "(((" + Digits + "(\\.)?(" + Digits + "?)(" + Exp + ")?%?)|" +

            // . Digits ExponentPart_opt FloatTypeSuffix_opt
            "(\\.(" + Digits + ")(" + Exp + ")?)|" +

            // Hexadecimal strings
            "((" +
            // 0[xX] HexDigits ._opt BinaryExponent FloatTypeSuffix_opt
            "(0[xX]" + HexDigits + "(\\.)?)|" +

            // 0[xX] HexDigits_opt . HexDigits BinaryExponent
            // FloatTypeSuffix_opt
            "(0[xX]" + HexDigits + "?(\\.)" + HexDigits + ")" +

            ")[pP][+-]?" + Digits + "))" + "[fFdD]?))" + "[\\x00-\\x20]*"), Pattern.CASE_INSENSITIVE);// Optional
                                                                           // trailing
                                                                           // "whitespace"

    public static void main(String[] args) {
        String[] values = new String[] { "asf 21", "124", "13.12 asfa", "test.html", "ab.html" };
        NumberFilter nf = new NumberFilter();
        for(String v : values) {
            String cleaned = nf.removeNumbers(v);
            System.out.println(String.format("%s --> %s",v, cleaned));
        }
    }

    public NumberFilter() {
    }

    public String removeNumbers(String text) {
        Matcher m = numberPattern.matcher(text);
        // if(m.matches()) {
            text = m.replaceAll("");
            if(text.trim().length()==0) {
                text=null;
            }
        // }
        return text;
    }

    public void cleanTable(Table t) {
        for(TableColumn c : t.getColumns()) {
            if(c.getDataType()==DataType.string) {
                for(TableRow r : t.getRows()) {
                    Object obj = r.get(c.getColumnIndex());
                    if(obj!=null) {
                        if(!obj.getClass().isArray()) {
                            String value = (String)obj;
                            Matcher m = numberPattern.matcher(value);
                            value = m.replaceAll("");
                            if(value.trim().length()==0) {
                                value=null;
                            }
                            r.set(c.getColumnIndex(), value);
                        } else {
                            Object[] list = (Object[]) obj;
                            for(int i = 0; i < list.length; i++) {
                                Object o = list[i];
                                if(o!=null) {
                                    String value = (String)o;
                                    Matcher m = numberPattern.matcher(value);
                                    value = m.replaceAll("");
                                    if(value.trim().length()==0) {
                                        value=null;
                                    }
                                    r.set(c.getColumnIndex(), value);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}