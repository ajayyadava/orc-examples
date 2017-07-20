package com.ajay.orc.examples;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

public class OrcReaderUsingVectorizedRowBatch {

  public static void main(String[] args) {
    long totalX = 0;
    long totalY = 0;

    try {
      Configuration conf = new Configuration();
      TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int>");
      Reader reader = OrcFile
          .createReader(new Path("/tmp/simple-my-file.orc"), OrcFile.readerOptions(conf));

      // read row batch column by column and then print it out

      RecordReader recordReader = reader.rows();

      // read schema from the file and get a row batch buffer
      VectorizedRowBatch batch = reader.getSchema().createRowBatch();

      // read the next batch in the buffer 'batch'
      while (recordReader.nextBatch(batch)) {
        // this creates readers for each of the columns, now you read from those columns
        LongColumnVector colX = (LongColumnVector) batch.cols[0];
        LongColumnVector colY = (LongColumnVector) batch.cols[1];

        for (int i=0; i < batch.size; i++) {
          totalX += colX.vector[i];
          totalY += colY.vector[i];
        }
      }
      // how can you read the meta???
    } catch (Exception e) {

    }
    System.out.println("totalX = " + totalX);
    System.out.println("totalY = " + totalY);
  }

}
