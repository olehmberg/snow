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
package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ClassHierarchy {

	private Set<String> rootNodes;
	private Map<String, LinkedList<String>> indexedTree;
	private Map<String, Integer> hierarchyLevels;
	private Map<String, String> classHierarchy;
	
	private Map<Integer, String> classIdToClassName;
	private Map<String, Integer> classNameToClassId;
	
	/**
	 * @return the hierarchyLevels
	 */
	public Map<String, Integer> getHierarchyLevels() {
		return hierarchyLevels;
	}
	
	/**
	 * @return the rootNodes
	 */
	public Set<String> getRootNodes() {
		return rootNodes;
	}
	
	/**
	 * @return the indexedTree
	 */
	public Map<String, LinkedList<String>> getIndexedTree() {
		return indexedTree;
	}
	
	/**
	 * @return the classIdToClassName
	 */
	public Map<Integer, String> getClassIdToClassName() {
		return classIdToClassName;
	}
	
	/**
	 * @return the classNameToClassId
	 */
	public Map<String, Integer> getClassNameToClassId() {
		return classNameToClassId;
	}
	
	public ClassHierarchy(KnowledgeBase kb) {
		this.classHierarchy = KnowledgeBase.getClassHierarchy();
		this.classIdToClassName = kb.getClassIndices();
		this.classNameToClassId = kb.getClassIds();
		
		// build the complete class hierarchy tree
		indexedTree = new HashMap<>();
		// keep track of the root nodes, as the class hierarchy does not contain owl:Thing
		rootNodes = new HashSet<>(classHierarchy.values());
		for(String subClass : classHierarchy.keySet()) {
			String superClass = classHierarchy.get(subClass);
			
			LinkedList<String> children = MapUtils.get(indexedTree, superClass, new LinkedList<String>());
			children.add(subClass);
			
			rootNodes.remove(subClass);
		}
		
		// create the hierarchy levels for each class
		hierarchyLevels = new HashMap<>();
		
		for(String cls : rootNodes) {
			createHierarchyLevels(cls, null);
		}
	}
	
	protected void createHierarchyLevels(String node, String parent) {
		Integer level = hierarchyLevels.get(parent);
		
		if(level==null) {
			level = 0;
		} else {
			level = level + 1;
		}
		
		hierarchyLevels.put(node, level);
		
		if(indexedTree.containsKey(node)) {
			for(String child : indexedTree.get(node)) {
				createHierarchyLevels(child, node);
			}
		}
	}
	
	public boolean isSubClassOf(Integer cls1, Integer cls2) {
		return isSubClassOf(
				classIdToClassName.get(cls1),
				classIdToClassName.get(cls2)
				);
	}
	
	public boolean isSubClassOf(String cls1, String cls2) {
		String superCls = cls1;
		while((superCls = classHierarchy.get(superCls)) != null){
			if(superCls.equals(cls2)){
				return true;
			}
		}
		return false;
	}
	
	public Set<String> getSuperClasses(String cls) {
		Set<String> superClasses = new HashSet<>();
		while((cls = classHierarchy.get(cls)) != null) {
			superClasses.add(cls);
		}
		return superClasses;
	}
}
