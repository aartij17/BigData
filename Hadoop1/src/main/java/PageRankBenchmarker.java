import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

public class PageRankBenchmarker {
    public static final  String BASE_URI ="hdfs://localhost:9000/";
    public static void main(String[] args) throws Exception {
        int[] dimensions={5,10,50,75};
        double[] sparsities={0.1,0.5,0.9};
        BasicConfigurator.configure();
        LogManager.getRootLogger().setLevel(Level.OFF);
        long start,end;
        for(int arg0: dimensions)
            for(double arg1:sparsities )
            {
                //puts the two files in ~/mm
                Process exec = Runtime.getRuntime().exec("python /home/kai/gen.py" + " " + arg0 + " " + arg1);
                exec.waitFor();
                ToolRunner.run(new HdfsWriter(), new String[]{"/home/kai/mm2","/files/mm2"});

                //Now to time all three approaches
                start=System.currentTimeMillis();
                MatrixMultiplyMR.computeConvergence(BASE_URI+"files/mm2/a.txt", BASE_URI+"files/mm2/b.txt",  BASE_URI+"files/mm2/c.txt",arg0,1,arg0);
                end=System.currentTimeMillis();
                System.out.println(arg0+","+arg1+",Convergence,"+(end-start));

                ToolRunner.run(new HdfsWriter(), new String[]{"/home/kai/mm2","/files/mm2"});


                start=System.currentTimeMillis();
                MouliMR.computeConvergence(BASE_URI+"files/mm2/a.txt", BASE_URI+"files/mm2/b.txt",  BASE_URI+"files/mm2/c.txt",arg0,arg0,arg0);
                end=System.currentTimeMillis();
                System.out.println(arg0+","+arg1+",Exponentiation,"+(end-start));




            }
    }
}
