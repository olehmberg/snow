# **S**ynthesizing **N**-ary relations fr**o**m **W**eb tables (SNoW)

*Under Construction - Source code will be available soon.*

Web tables have been used in many approaches to extend existing data sources with additional data. 
But so far, all approaches are limited to the extraction of binary relations. 
We find, however, that a large number of web tables actually are n-ary relations. 
This means that, in many cases, the data can only be understood if more than two columns of the web tables are considered.

The SNoW system identifies and synthesizes n-ary relations from web tables and integrates them with a knowledge base.

Given a set of web tables and a target knowledge base, the SNoW system applies schema matching and functional dependency discovery algorithms to create an integrated schema.
The schema of the target knowledge base is used as a starting point for the integrated schema, which is then extended by the relations obtained from the web tables and their functional dependencies.
The result is a star-shaped relational schema, with the classes from the knowledge base at the centres and additional relations from the web tables referring to them.

## System Overview

The following figure visualises the integration process of the SNoW system.
It consists of several steps, which are briefly described in the following.
The first step consists of stitching the web tables based on their column headers into *union tables*, and extracting data from the context of the web tables, such as the URL or page title, into additional columns of the union tables.
Then, several schema matchers are employed to create a schema mapping among the union tables and between the union tables and the target knowledge base.
Afterwards, the schema mapping is used to stitch the union tables into larger *stitched union tables*, which are then used to discover functional dependencies.
Finally, a last stitching step creates a *stitched universal relation* from all stitched union tables, which is then normalised using the functional dependencies discovered earlier to create the integrated schema.

![SNoW Process Overview](/img/overview.PNG)

As an example, consider the web table in the next figure, which contains employment statistics for a certain occupation in several u.s. states at a given time.
While a human can easily understand this, it is hard for an algorithm, as the table contains only very few examples (the figure shows the complete table) and important attributes such as the occupation and the date are missing from the table.
But, by considering all web tables from the same web site and the context surrounding them on the original web pages, the SNoW system is able to discover the correct functional dependency *\{state, occupation, year, month\}&rarr;\{employment\}* and synthesizes an n-ary relation containing all these attributes.
Existing methods, however, would extract this data as binary relation *\{state,employment\}*, resulting in about 2\,000 different employment numbers for a single state, without any additional information, for the web site in the example.

![Example Web Table](/img/example_table.PNG)

## Project Structure

### SNoW Datasets

The datasets and annotations used for evaluation can be found in the `datasets/` directory.
Every sub-directory is a dataset for a single web site and contains the pre-processed union tables in `union_dedup_json/` and the annotations in the `evaluation/` sub-directory.

Dataset statistics: $A_O$=original attributes, $A_T$=total attributes (original \& context attributes), $A_U$=universal attributes, Cor.=correspondences among attributes in union tables.

<table>
<thead><tr><th></th><th colspan=3>Web Tables</th><th colspan=4>Clustered Union Tables</th><th colspan=4>Annotation</th></tr></thead><tbody>
 <tr><td>Host</td><td>Tables</td><td>Columns</td><td>Rows</td><td>Tables</td><td>A<sub>O</sub></td><td>A<sub>T</sub></td><td>Rows</td><td>Classes</td><td>A<sub>U</sub></td><td>Cor.</td><td>FDs</td></tr>
 <tr><td>d3football.com</td><td> 40,584   </td><td> 233,105   </td><td> 103,404   </td><td> 12   </td><td> 63   </td><td> 145   </td><td> 11,731   </td><td>2</td><td>41</td><td> 473   </td><td>18</td></tr>
 <tr><td>data.bls.gov</td><td> 10,824   </td><td> 59,087   </td><td> 54,193   </td><td> 12   </td><td> 61   </td><td> 181   </td><td> 52,629   </td><td>1</td><td>51</td><td> 459   </td><td>12</td></tr>
 <tr><td>flightaware.com</td><td> 2,888   </td><td> 13,712   </td><td> 19,820   </td><td> 6   </td><td> 22   </td><td> 72   </td><td> 19,613   </td><td>1</td><td>35</td><td> 67   </td><td>13</td></tr>
 <tr><td>itunes.apple.com</td><td> 42,729   </td><td> 258,275   </td><td> 494,302   </td><td> 76   </td><td> 470   </td><td> 1,095   </td><td> 491,977   </td><td>1</td><td>59</td><td> 30,809   </td><td>6</td></tr>
 <tr><td>seatgeek.com</td><td> 157,578   </td><td> 630,051   </td><td> 2,644,032   </td><td> 72   </td><td> 266   </td><td> 714   </td><td> 300,106   </td><td>6</td><td>71</td><td> 17,542   </td><td>30</td></tr>
 <tr><td>www.amoeba.com</td><td> 5,529   </td><td> 18,224   </td><td> 44,504   </td><td> 65   </td><td> 227   </td><td> 712   </td><td> 32,898   </td><td>2</td><td>42</td><td> 18,433   </td><td>13</td></tr>
 <tr><td>www.cia.gov</td><td> 30,569   </td><td> 121,460   </td><td> 6,221,350   </td><td> 213   </td><td> 562   </td><td> 2,225   </td><td> 70,809   </td><td>1</td><td>323</td><td> 182,833   </td><td>181</td></tr>
 <tr><td>www.nndb.com</td><td> 23,522   </td><td> 116,716   </td><td> 231,738   </td><td> 29   </td><td> 123   </td><td> 299   </td><td> 229,445   </td><td>6</td><td>29</td><td> 3,403   </td><td>10</td></tr>
 <tr><td>www.vgchartz.com</td><td> 23,258   </td><td> 116,285   </td><td> 58,637   </td><td> 8   </td><td> 39   </td><td> 87   </td><td> 33,715   </td><td>1</td><td>36</td><td> 140   </td><td>13</td></tr>
 <tr><td>Sum</td><td> 337,481   </td><td> 1,566,915   </td><td> 9,871,980   </td><td> 493   </td><td> 1,833   </td><td> 5,530   </td><td> 1,242,923   </td><td>14</td><td>687</td><td> 254,159   </td><td>296</td></tr>
</tbody></table>

### SNoW Evaluation

We run the SNoW system on our datasets in different configurations.
The first two configurations do not apply any matchers and use the schema correspondences from the ground truth.

- *Baseline (B)* detects an entity label column in each table and assumes all other attributes only depend on this column. 
This configuration resembles existing approaches which only extract binary relations.

- *Stitching (S)* the SNoW system with FD discovery.
We use this configuration to evaluate the results of the FD discovery and stitching procedures without the influence of errors introduced by the matching components.

- *Matching (M)* the SNoW system, using the matching components instead of the correspondences from the ground truth.

<table>
<thead><tr><th></th><th colspan=3>Schema Matching </th><th colspan=5>Schema Extension</th><th colspan=4>Reference</th></tr></thead><tbody>
 <tr><td>&nbsp;</td><td colspan=3>M</td><td>B</td><td>S</td><td colspan=3>M</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td></tr>
 <tr><td>&nbsp;</td><td>P</td><td>R</td><td>F1</td><td>F1</td><td>F1</td><td>P</td><td>R</td><td>F1</td><td>Cor.</td><td>Entities</td><td>Tuples</td><td>Values</td></tr>
 <tr><td>d3football.com</td><td>0.66</td><td>0.95</td><td>0.78</td><td>0.49</td><td>0.94</td><td>0.91</td><td>0.82</td><td>0.86</td><td>1.39E+09</td><td>2,766</td><td>40,011</td><td>150,129</td></tr>
 <tr><td>data.bls.gov</td><td>0.87</td><td>0.92</td><td>0.89</td><td>0.42</td><td>0.98</td><td>0.95</td><td>0.92</td><td>0.93</td><td>4.21E+08</td><td>57</td><td>217,588</td><td>1,038,019</td></tr>
 <tr><td>flightaware.com</td><td>0.97</td><td>0.99</td><td>0.98</td><td>0.46</td><td>1.00</td><td>0.96</td><td>0.86</td><td>0.91</td><td>2.56E+07</td><td>89</td><td>46,851</td><td>187,548</td></tr>
 <tr><td>itunes.apple.com</td><td>0.92</td><td>0.75</td><td>0.83</td><td>0.68</td><td>0.90</td><td>0.81</td><td>0.77</td><td>0.79</td><td>8.94E+09</td><td>265,431</td><td>1,703,654</td><td>6,788,752</td></tr>
 <tr><td>seatgeek.com</td><td>1.00</td><td>1.00</td><td>1.00</td><td>0.99</td><td>0.99</td><td>0.99</td><td>0.94</td><td>0.97</td><td>4.95E+10</td><td>30,152</td><td>55,044</td><td>133,918</td></tr>
 <tr><td>www.amoeba.com</td><td>1.00</td><td>0.87</td><td>0.93</td><td>0.81</td><td>1.00</td><td>0.99</td><td>0.94</td><td>0.96</td><td>8.57E+07</td><td>23,348</td><td>61,658</td><td>222,481</td></tr>
 <tr><td>www.cia.gov</td><td>0.96</td><td>0.99</td><td>0.98</td><td>0.82</td><td>0.94</td><td>0.73</td><td>0.67</td><td>0.70</td><td>1.88E+09</td><td>267</td><td>48,076</td><td>131,014</td></tr>
 <tr><td>www.nndb.com</td><td>1.00</td><td>1.00</td><td>1.00</td><td>1.00</td><td>0.93</td><td>1.00</td><td>0.87</td><td>0.93</td><td>1.35E+09</td><td>40,003</td><td>133,848</td><td>343,476</td></tr>
 <tr><td>www.vgchartz.com</td><td>1.00</td><td>1.00</td><td>1.00</td><td>0.45</td><td>0.89</td><td>1.00</td><td>0.80</td><td>0.89</td><td>1.62E+09</td><td>11,711</td><td>108,066</td><td>396,959</td></tr>
 <tr><td>Avg. / Sum</td><td>0.93</td><td>0.94</td><td>0.93</td><td>0.68</td><td>0.95</td><td>0.93</td><td>0.84</td><td>0.88</td><td>6.52E+10</td><td>373,824</td><td>2,414,796</td><td>9,392,296</td></tr>
</tbody></table>

### Running SNoW

After cloning the repository, use the `unzip` script to extract the datasets:

`./unzip`

Before running snow, the paths in `SET_VARS` must be adjusted for your environment:

```bash
J8="path to your java 8 executable"
JAR="path to the SNoW jar and its dependencies"
VMARGS="-Xmx300G"
```

To run the snow system in different configurations, use the one of following scripts, which accept the path to the dataset as parameter:

`./run_snow datasets/d3football.com`

- `run_snow_b`: Runs the snow system in configuration B
- `run_snow`: Runs the snow system in configuration S
- `run_snow_match`: Runs the snow system in configuration M
- `run_snow_reference`: Creates a reference extraction based on the annotations

To run the value-based evaluation, use `evaluate_containment` and provide the path to the dataset as parameter:

`./evaluate_containment datasets/d3football.com`

Or use `evaluate_containment_all` to run the evaluation on all datasets and configurations.

### Creating New Datasets

To create new datasets, the clustered union tables must be created before running SNoW. Copy all web tables for a single web site in a directory and use `create_clustered_union` to create the union tables:

`./create_clustered_union directory_containing_a_single_web_site`

If you want to create the union tables for multiple web sites, create a directory with one sub-directory for every web site and run `run_clustered_union` with the directory as parameter:

`./run_clustered_union directory_containing_all_web_sites`

During the creation of the union tables, a directory `clustered_union_correspondences` is created, which contains schema correspondences among the generated context columns. These correspondences must be copied into the `evaluation` sub-directory of the dataset using the file name `context_correspondences.tsv`.
