package com.ajay.orc.examples;


import java.util.Arrays;
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
      System.out.println("CompressionKind = " + reader.getCompressionKind());
      System.out.println(" ============================================== " );
      System.out.println("CompressionSize = " + reader.getCompressionSize());
      System.out.println(" ============================================== " );
      System.out.println("ContentLength = " + reader.getContentLength());
      System.out.println(" ============================================== " );
      System.out.println("fileTail = " + reader.getFileTail());
      System.out.println(" ============================================== " );

      // FileVersion , major and minor
      System.out.println(String
          .format("fileVersion:\n name: {}, major: {}, minor: {} fileVersion = ",
              reader.getFileVersion().getName(), reader.getFileVersion().getMajor(),
              reader.getFileVersion().getMinor()));
      System.out.println(" ============================================== " );

      System.out.println("postscript = " + reader.getFileTail().getPostscript());
      System.out.println(" ============================================== " );
      System.out.println("footer = " + reader.getFileTail().getFooter());
      System.out.println(" ============================================== " );
      System.out.println("no. of rows = " + reader.getNumberOfRows());
      System.out.println(" ============================================== " );
      System.out.println("raw data size of cols = " + reader.getRawDataSizeOfColumns(Arrays.asList("x", "y")));
      System.out.println(" ============================================== " );
      System.out.println("data size from col indices = " + reader.getRawDataSizeFromColIndices(Arrays.asList(0, 1)));
      System.out.println(" ============================================== " );

      System.out.println("metadata keys = " + reader.getMetadataKeys());
      System.out.println(" ============================================== " );
      System.out.println("reader.getStatistics() = " + reader.getStatistics());
      System.out.println(" ============================================== " );
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



    } catch (Exception e) {

    }
    System.out.println("totalX = " + totalX);
    System.out.println("totalY = " + totalY);
  }

}
