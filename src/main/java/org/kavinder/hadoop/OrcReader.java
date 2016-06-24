package org.kavinder.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;


import java.io.IOException;

public class OrcReader {

    public static void main(String []args) throws IOException{
        Configuration conf = new Configuration();
        conf.addResource(new Path("file:////Users/kdhaliwal/workspace/singlecluster-HDP/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("file:////Users/kdhaliwal/workspace/singlecluster-HDP/hadoop/etc/hadoop/hdfs-site.xml"));
        Path path = new Path("/hive/warehouse/temps_orc/000000_0");
        Reader reader = OrcFile.createReader(path,
                            OrcFile.readerOptions(conf));
        TypeDescription schema = reader.getSchema();
        boolean [] include = new boolean[schema.getMaximumId() + 1]; // + 1 because col ids start at 1
        TypeDescription col0 = schema.getChildren().get(0);
        TypeDescription col5 = schema.getChildren().get(5);
        include[col0.getId()] = true;
        include[col5.getId()] = true;
        RecordReader rows = reader.rows(new Reader.Options().include(include));
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        while (rows.nextBatch(batch)) {
            for (int r = 0; r < batch.size; ++r) {
                BytesColumnVector vector = (BytesColumnVector) batch.cols[0];
                BytesColumnVector vector5 = (BytesColumnVector) batch.cols[5];
                System.out.println(vector.toString(r) + " | " + vector5.toString(r));
            }
        }

    }
}
