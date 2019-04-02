# **S**ynthesizing **N**-ary relations fr**o**m **W**eb tables (SNoW)

*Under Construction - Source code will be available soon.*

Web tables have been used in many approaches to extend existing data sources with additional data. 
But so far, all approaches are limited to the extraction of binary relations. 
We find, however, that a large number of web tables actually are n-ary relations. 
This means that, in many cases, the data can only be understood if more than two columns of the web tables are considered.

The SNoW system identifies and synthesizes n-ary relations from web tables and integrates them with a knowledge base.

Given a set of web tables and a target knowledge base, the SNoW method extends each web table with additional context columns, stitches matching web tables into larger tables, and applies functional dependency discovery to identify the relations that are represented in the web tables. Further, it normalises the stitched tables, guided by the schema of the knowledge base, to create an integrated schema.

## System Overview

The following figure visualises the integration process of the SNoW system.
First, each web table is extended with context columns, which are generated from the web page that contains the web table. Then, web tables with identical schema are stitched into *union tables*.
These union tables contain more tuples than the original web tables and improve the performance of schema matchers, which are executed in the next step. 
The schema matchers create a mapping among the union tables, to identify matching schemata which are expressed with different column headers, and a mapping from the union tables to the knowledge base. 
Based on the mapping among the union tables, the stitching is repeated to merge all union tables
with matching schemata into *stitched union tables*. 
The stitched union tables contain the tuples of all web tables with the respective schema and enable the discovery of functional dependencies, which is infeasible on the much smaller original tables. 
Finally, all stitched union tables which are mapped to the same knowledge base class are merged into a *stitched universal relation*, which is normalised to create an integrated schema. 
This normalisation is guided by the mapping to the knowledge base and produces a star-shaped schema with the existing classes from the knowledge base at the centres and the n-ary relations that were discovered in the web tables referring to them.

![SNoW Process Overview](/img/overview.PNG)

As an example, consider the web table in the next figure, which contains employment statistics for a certain profession in several U.S. states at a given time. 
While a human can easily understand this, it is hard for an algorithm, as the table contains only very few examples (the figure shows the complete table) and important attributes, such as the profession and the date, are missing from the table.
Existing approaches use recognisers or matchers to find column pairs which represent an existing or an unknown binary relation. 
In the example, such systems would extract a binary relation {state, employment}, which can be found in thousands of web tables from the same web site. 
As a result, the extracted binary relation contains about 2 000 different employment numbers for a single state and no possibility to choose a single, correct value, or to understand any individual value. 
To extract a meaningful relation for the employment attribute, we must include all attributes which determine its value, i.e., discover the functional dependency {state, page title, URI 1, URI 2}&rarr;{employment}. 
This is only possible if we take additional data from the context of the web table into account and combine the observations from multiple web tables, which contain employment numbers for  different professions and dates.

![Example Web Table](/img/example_table.PNG)

## Project Structure

### SNoW Datasets

The datasets and annotations used for evaluation can be found in the `datasets/` directory.
Every sub-directory is a dataset for a single web site and contains the pre-processed union tables in `union_dedup_json/` and the annotations in the `evaluation/` sub-directory.

### SNoW Evaluation

We run the SNoW system on our datasets in different configurations.
The first two configurations do not apply any matchers and use the schema correspondences from the ground truth.

- *Binary* detects an entity label column in each table and assumes all other attributes only depend on this column. 
This configuration uses the manually annotated schema mapping as input and extracts a binary relation for each reference relation and resembles a perfect extractor for binary relations as baseline.

- *N-Ary* uses functional dependency discovery instead of binary relations, but does not generate context attributes.

- *N-Ary + C*  uses functional dependency discovery and generates context attributes.

- *Match*: The SNoW system (N-ary + C), using the matching components instead of the correspondences from the annotations.

- *Norm*: The end-to-end configuration of the SNoW system. Uses functional dependency discovery, generates context attributes, uses the matching components and applies normalisation.

Table 1: Dataset statistics and Evaluation. A_O=original attributes, A_T=total attributes (original \& context attributes), A_U=universal attributes.

<table>
<thead><tr><th></th><th>Web Tables</th><th colspan="3">Union Tables</th><th colspan="2">Annotation</th><th colspan="6">Experiments - F1-measure</th></tr>
 <tr><td>Host</td><td>Tables</td><td>Tables</td><td>A<sub>O</sub></td><td>A<sub>T</sub></td><td>A<sub>U</sub></td><td>FDs</td><td>Schema</td><td>Binary</td><td>N-Ary</td><td>N-ary +C</td><td>Match</td><td>Norm</td></tr></thead><tbody>
 <tr><td>d3football.com</td><td> 40,584   </td><td> 12   </td><td> 63   </td><td> 145   </td><td>41</td><td>18</td><td>0.779</td><td>0.495</td><td>0.480</td><td>0.851</td><td>0.765</td><td>0.765</td></tr>
 <tr><td>data.bls.gov</td><td> 10,824   </td><td> 12   </td><td> 61   </td><td> 181   </td><td>51</td><td>12</td><td>0.895</td><td>0.415</td><td>0.485</td><td>0.984</td><td>0.900</td><td>0.900</td></tr>
 <tr><td>flightaware.com</td><td> 2,888   </td><td> 6   </td><td> 22   </td><td> 72   </td><td>35</td><td>13</td><td>0.982</td><td>0.459</td><td>0.405</td><td>1.000</td><td>0.942</td><td>0.733</td></tr>
 <tr><td>itunes.apple.com</td><td> 42,729   </td><td> 76   </td><td> 470   </td><td> 1,095   </td><td>59</td><td>6</td><td>0.827</td><td>0.680</td><td>0.674</td><td>0.953</td><td>0.809</td><td>0.763</td></tr>
 <tr><td>seatgeek.com</td><td> 157,578   </td><td> 72   </td><td> 266   </td><td> 714   </td><td>71</td><td>30</td><td>0.999</td><td>0.988</td><td>0.992</td><td>0.984</td><td>0.962</td><td>0.962</td></tr>
 <tr><td>www.amoeba.com</td><td> 5,529   </td><td> 65   </td><td> 227   </td><td> 712   </td><td>42</td><td>13</td><td>0.933</td><td>0.812</td><td>0.394</td><td>0.907</td><td>0.885</td><td>0.885</td></tr>
 <tr><td>www.cia.gov</td><td> 30,569   </td><td> 213   </td><td> 562   </td><td> 2,225   </td><td>323</td><td>162</td><td>0.976</td><td>0.858</td><td>0.836</td><td>0.821</td><td>0.660</td><td>0.635</td></tr>
 <tr><td>www.nndb.com</td><td> 23,522   </td><td> 29   </td><td> 123   </td><td> 299   </td><td>29</td><td>10</td><td>1.000</td><td>1.000</td><td>1.000</td><td>1.000</td><td>0.999</td><td>0.999</td></tr>
 <tr><td>www.vgchartz.com</td><td> 23,258   </td><td> 8   </td><td> 39   </td><td> 87   </td><td>36</td><td>13</td><td>1.000</td><td>0.448</td><td>0.253</td><td>1.000</td><td>1.000</td><td>1.000</td></tr>
 <tr><td>Sum / Macro Avg.</td><td> 337,481   </td><td> 493   </td><td> 1,833   </td><td> 5,530   </td><td>687</td><td>277</td><td>0.932</td><td>0.684</td><td>0.613</td><td>0.944</td><td>0.880</td><td>0.849</td></tr>
</tbody></table>

### Running SNoW

After cloning the repository, use the `unzip` script to extract the datasets:

`./unzip`

Before running snow, the paths in `SET_VARS` must be adjusted for your environment:

```bash
J8="path to your java 8 executable"
JAR="path to the SNoW jar and its dependencies"
VMARGS="-Xmx300G"
TANE_HOME="path to the TANE implementation"
```

Before running SNoW, you must download the implementation of the [TANE algorithm](https://www.cs.helsinki.fi/research/fdk/datamining/tane/) on your machine.

To run the snow system in different configurations, use one of the following scripts, which accept the path to the dataset as parameter:

`./run_snow datasets/d3football.com`

- `run_snow_b`: configuration *Binary*
- `run_snow_noContextColumns`: configuration *N-Ary*
- `run_snow`: configuration *N-Ary + C*
- `run_snow_match`: configuration *Match*
- `run_snow_match_normalised`: configuration *Norm*
- `run_snow_reference`: Creates a reference extraction based on the annotations

To run the evaluation, use `evaluate_containment` and provide the path to the dataset as parameter:

`./evaluate_containment datasets/d3football.com`

Or use `evaluate_containment_all` to run the evaluation on all datasets and configurations.

### Creating New Datasets

To create new datasets, the clustered union tables must be created before running SNoW. Copy all web tables for a single web site in a directory and use `create_clustered_union` to create the union tables:

`./create_clustered_union directory_containing_a_single_web_site`

If you want to create the union tables for multiple web sites, create a directory with one sub-directory for every web site and run `run_clustered_union` with the directory as parameter:

`./run_clustered_union directory_containing_all_web_sites`

During the creation of the union tables, a directory `clustered_union_correspondences` is created, which contains schema correspondences among the generated context columns. These correspondences must be copied into the `evaluation` sub-directory of the dataset using the file name `context_correspondences.tsv`.
