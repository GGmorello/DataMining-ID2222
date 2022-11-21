# Discovery of Frequent Itemsets

This folder contains the source code for the second homework in ID2222 - Data Mining.

Prerequisites:
- Install `scala@2.12.15`
- Install `spark@3.3.1`
- Install `Java JDK 17/19`
- Install `sbt` (compatible version, might be included with Scala/Spark)

This might require the installation of Spark locally. We used spark version `3.3.1`, as see in the `build.sbt` file.

To run the code, do the following:

- Navigate to the "Assignment2" folder
- Run the following command: `sbt package`
- Followed by the following: `spark-submit --class "Main" --master local target/scala-2.12/simple-project_2.12-1.0.jar 3 1000 0.5`


This will run the code with parameters:
- `args(0)` => k = 3 (passes through the dataset, i.e. max frequent itemset tuple size)
- `args(1)` => support = 1000 (amount of occurences of itemsets to filter out)
- `args(2)` => confidence = 0.5 (threshold for confidence of generated association rules)

The output will be similar to the following:
<pre>
Frequent itemsets - k: 3 support: 1000 confidence: 0.5
A-Priori result: 1
tuple: (39,704,825), support: 1035

Association rules:
tuple: (39,704,825) rule: [704] -> [39,825]; confidence: 0.5769230769230769
tuple: (39,704,825) rule: [39,704] -> [825]; confidence: 0.9349593495934959
tuple: (39,704,825) rule: [39,825] -> [704]; confidence: 0.8719460825610783
tuple: (39,704,825) rule: [704,825] -> [39]; confidence: 0.9392014519056261
</pre>