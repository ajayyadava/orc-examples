ORC-Examples
=======

To run do the following

1. mvn clean install from the orc-example(top-level) directory.
2. Then do 
 * cd orc-examples/
 * mvn exec:java -Dexec.mainClass="com.ajay.orc.examples.SimpleOrcWriterUsingVectorizedRowBatch"
 
 This will run the main method of that class and write an orc-file at /tmp/simple-my-file.orc 
 You can change class name to other classes with main method in that module to run those classes.


