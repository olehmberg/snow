package de.uni_mannheim.informatik.dws.tnt.match.dependencies;

import java.util.Collection;

import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

public class ApproximateFunctionalDependency {

	private Collection<TableColumn> determinant;
	private TableColumn dependant;
	private Collection<Pair<TableRow, TableRow>> violations;
	private double approximationRate;
	public Collection<TableColumn> getDeterminant() {
		return determinant;
	}
	public TableColumn getDependant() {
		return dependant;
	}
	public Collection<Pair<TableRow, TableRow>> getViolations() {
		return violations;
	}
	public double getApproximationRate() {
		return approximationRate;
	}
	
	public ApproximateFunctionalDependency(Collection<TableColumn> determinant, TableColumn dependant,
			Collection<Pair<TableRow, TableRow>> violations, double approximationRate) {
		super();
		this.determinant = determinant;
		this.dependant = dependant;
		this.violations = violations;
		this.approximationRate = approximationRate;
	}

}
