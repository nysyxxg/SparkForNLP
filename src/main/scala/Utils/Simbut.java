package Utils;

import org.apache.spark.deploy.SparkSubmit;

/**
 * Created by MingDong on 2016/12/10.
 */
public class Simbut {
    public static void main(String[] args) {
        String[] argss = new String[]{
                "--master", "spark://172.16.110.109:7077",
                "--deploy-mode", "client",
                "--name", "javasubmit",
                "--executor-memory", "1G",
                "--total-executor-cores", "2",
                "--class", "spark.Test2",
                //"--jars","hdfs://mycluster/test/spark12.jar",
                "hdfs://mycluster:8020/test/ryf3.jar",
                //"build/ryf3.jar"

        };
        SparkSubmit.main(argss);
    }
}
