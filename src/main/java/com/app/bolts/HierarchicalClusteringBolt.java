package com.app.bolts;

import com.app.models.Cluster;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Mohammad ALSHAER <malshaer at LYON && Beirut>
 */
public class HierarchicalClusteringBolt extends BaseRichBolt{
    private String name;
    private int counter = 1;
    private int reccount = 0;
    private int min = Integer.MAX_VALUE;
    private int x = 0;
    private int y = 0;
    private int headercount = 0;
    private int headersforzeros = 0;
    private List<String> headers;
    private List<String> Lines;
    private List<List<String>> records;
    private String[][] datamatrix;
    private String[][] comparematrix;
    private String[][] originalcomparematrix;
    private String[][] tempmatrix;
    public List<Cluster> clusters;
    private OutputCollector collector;

   @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        headers = new ArrayList<String>();
        Lines = new ArrayList<String>();
        records = new ArrayList<List<String>>();
        datamatrix = new String[500][500];
        originalcomparematrix = new String[500][500];
        clusters = new ArrayList<Cluster>();
    }

    @Override
    public void execute(Tuple tuple) {
        // extract the record from the tuple
        processRecord(tuple.getStringByField("record"));

        if(!clusters.isEmpty() && clusters.size()>0) {
            StringBuilder sb = new StringBuilder();
            print(clusters.get(clusters.size() - 1), sb);
            collector.emit(new Values(sb.toString()));
        }
    }

   @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("clusters"));
    }

    public void print(Cluster x, StringBuilder sb) {
        boolean process = true;
      /*  for (Cluster c : clusters) {
            if (c.name.equals(x.name)) {
                for (String rr : x.record) {
                    if (rr.startsWith("rec"))
                        process = true;
                }
            }
        }*/
        if (process) {
            sb.append(x.name + " ");
            //System.out.print(x.name + " ");

            for (String value : x.record) {
                sb.append(value + " & ");
                //System.out.print(value + " ");
            }
            sb.replace(sb.length() - 3, sb.length(), "");
            sb.append("\n");
            //System.out.println();
        }

        for (String entry : x.record) {
            if (entry.startsWith("rec")) {
                List<String> sublist = x.record1.get(x.record.indexOf(entry));
                for (String value : sublist) {
                    sb.append(value + ", ");
                    //System.out.print(value + " ");
                }
                sb.replace(sb.length() - 2, sb.length(), "");
                sb.append("\n");
                //System.out.println();
            } else if (entry.startsWith("cluster")) {
                for (Cluster c : clusters) {
                    String c_name = c.name.replace(" ", "").replace(":", "").trim();
                    if (c_name.equals(entry.trim())) {
                        print(c, sb);
                    }
                }
            }
        }
    }

    public void processRecord(String record) {

        try {
            Lines.add(record);//adds each line from data file to the arraylist
            genHeaders(record);
            setdatamatrixheaders();
            setdatamatrixrecords();
            letsfillthematrix();
            setcomparematrix();
            fillcomparematrix();
            updatetempmatrix();

           /* for (int i = 0; i < clusters.size(); i++) {
                clusters.get(i).PrintCluster();
                clusters.get(i).PrintCluster1();
            }*/
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void genHeaders(String line) {
        List<String> temp = new ArrayList<String>();
        String[] splitted_line = line.split(",");
        String[] tempLine = new String[splitted_line.length - 1];
        for(int k=0; k< splitted_line.length-1 ;k++){
            tempLine[k] = splitted_line[k];
        }

        for (String s : splitted_line) {
            temp.add(s);
        }

        records.add(temp);//added the record

        for (int j = 0; j < tempLine.length; j++) {
            if (!headers.contains(tempLine[j])) {
                headers.add(tempLine[j]);//added all unique headers for each record
            }
        }
    }

    public void setdatamatrixheaders()//added only new headers to all the matrix
    {

        for (int i = headercount; i < headers.size(); i++) {
            datamatrix[0][i + 1] = headers.get(i);
        }
        headercount = headers.size();
    }

    public void setdatamatrixrecords()//added only new the new record
    {

        datamatrix[records.size()][0] = "rec" + (records.size());
    }

    public void letsfillthematrix()// added data only for the new record the rest fill with 0
    {

        for (int k = 0; k < ((List<String>) records.get(records.size() - 1)).size(); k++) {
            datamatrix[records.size()][headers.indexOf(records.get(records.size() - 1).get(k)) + 1] = "1";
        }//set the 1s done

        for (int k = 0; k < headers.size(); k++) {
            if (datamatrix[records.size()][k + 1] == null) {
                datamatrix[records.size()][k + 1] = "0";
            }
        }//set the zeros for the new record

        for (int i = 0; i < records.size() - 1; i++) {
            for (int k = headersforzeros; k < headers.size(); k++) {
                datamatrix[i + 1][k + 1] = "0";
            }
        }
        headersforzeros = headers.size();//set new zeros for old records

    }

    public void setcomparematrix()//added the new rec to both ends and keep this original and make 2 copies for clusters
    {

        originalcomparematrix[0][records.size()] = "rec" + (records.size());

        originalcomparematrix[records.size()][0] = "rec" + (records.size());
    }

    public void fillcomparematrix()//filled the distances between old and new rec
    { // this method is filling the distance matrix according to hamming distance
        int temp = 0;
        for (int j = 0; j < records.size(); j++) {
            if (records.size() == j + 1)//diagonal
            {
                originalcomparematrix[records.size()][j + 1] = "0";
                continue;
            }
            for (int k = 0; k < headers.size(); k++) {
                if (!datamatrix[records.size()][k + 1].equals(datamatrix[j + 1][k + 1])) {
                    temp = temp + 1;
                }
            }
            originalcomparematrix[records.size()][j + 1] = temp + "";
            temp = 0;
        }

        // filling data below diagonal same as above diagonal
        for (int j = 0; j < records.size(); j++) {
            if (records.size() == j + 1) {
                continue;
            }
            originalcomparematrix[j + 1][records.size()] = originalcomparematrix[records.size()][j + 1];
        }

        /* dataGridView2.ColumnCount = records.Count() + 1;
            for (int i = 0; i < records.Count() + 1; i++)
            {
                DataGridViewRow row = new DataGridViewRow();
                row.CreateCells(dataGridView2);
                for (int j = 0; j < records.Count() + 1; j++)
                    row.Cells[j].Value = comparematrix[i, j];
                dataGridView2.Rows.Add(row);
            }
            dataGridView2.Rows[0].Frozen = true;
            dataGridView2.Columns[0].Frozen = true;*/
        // comparematrix = (String[][]) originalcomparematrix.clone();
        // tempmatrix = (String[][]) comparematrix.clone();
        comparematrix = new String[500][500];
        tempmatrix = new String[500][500];
        copyDoubleArray(originalcomparematrix, comparematrix);
        copyDoubleArray(originalcomparematrix, tempmatrix);

    }

    public void findMin() {

        for (int i = 0; i < reccount; i++) {
            for (int j = 0; j < reccount; j++) {

                if (i + 1 == j + 1) {
                    continue;
                }
                int temp;
                try {

                    temp = Integer.parseInt(comparematrix[i + 1][j + 1]);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    continue;
                }
                if (temp < min) {
                    min = temp;
                    x = i + 1;
                    y = j + 1;

                }
                System.out.println("minimum = " + min);

            }
        }

    }

    public void Do_cluster() {
        name = "cluster " + counter + ":";
        List<String> r = new ArrayList<String>();
        r.add(comparematrix[x][0] + " ");
        r.add(comparematrix[y][0] + " ");

        String last_1 = "1";
        String last_2 = "1";

        Pattern p1 = Pattern.compile("\\d+");
        if (comparematrix[x][0] != null) {
            Matcher m1 = p1.matcher(comparematrix[x][0]);
            if (m1.find()) {
                last_1 = m1.group(0);
            }
        }
        if (comparematrix[y][0] != null) {
            Matcher m2 = p1.matcher(comparematrix[y][0]);
            if (m2.find()) {
                last_2 = m2.group(0);
            }
        }

        // clusters.add(new Cluster(name, r, records.get(x-1), records.get(y-1)));
        clusters.add(new Cluster(name, r, records.get(Integer.parseInt(last_1) - 1), records.get(Integer.parseInt(last_2) - 1)));

        /* clusters[clusters.Count()-1].PrintCluster();
            clusters[clusters.Count()-1].PrintCluster1();*/
        for (int i = 0, j = 0; i < tempmatrix[0].length; i++) {
            if (i == x || i == y) {
                continue;
            }

            for (int k = 0, u = 0; k < tempmatrix[1].length; k++) {
                if (k == y || k == x) {
                    continue;
                }

                tempmatrix[j][u] = tempmatrix[i][k];
                u++;
            }

            j++;
        }
        reccount = reccount - 1;

    }

    public void updatetempmatrix()//call when a new rec is added with all temp matrix and values, reset all after each clustering process is done(each line), new check min
    { // this method is updating the distance matrix according to hamming distance
        clusters.clear();
        reccount = records.size();
        counter = 1;
        while (reccount > 1) {

            findMin();
            /*if(min > 6){
                   min = int.MaxValue;
                   break;
               }*/
            Do_cluster();

            int temp = 1;
            int k = 1;
            for (int i = 0; i < reccount + 1; i++) {
                if (i + 1 == x || i + 1 == y) {
                } else {
                    int num1 = 0;
                    int num2 = 0;
                    if (comparematrix[i + 1][x] != null) {
                        num1 = Integer.parseInt(comparematrix[i + 1][x]);
                    }
                    if (comparematrix[i + 1][y] != null) {
                        num2 = Integer.parseInt(comparematrix[i + 1][y]);
                    }
                    temp = Math.min(num1, num2);
                    tempmatrix[k][reccount] = temp + "";
                    k++;
                }
            }

            for (int w = 0; w < records.size(); w++) {
                tempmatrix[0][reccount] = "cluster" + counter;
                tempmatrix[reccount][0] = "cluster" + counter;
                tempmatrix[reccount][reccount] = "0";
            }

            for (int i = 0; i < records.size(); i++) {
                for (int j = i; j < records.size(); j++) {
                    if (i + 1 == j + 1) {
                        continue;
                    }
                    tempmatrix[j + 1][i + 1] = tempmatrix[i + 1][j + 1];
                }
            }
            //comparematrix = null;
            //comparematrix = (String[][]) tempmatrix.clone();
            comparematrix = new String[500][500];
            copyDoubleArray(tempmatrix, comparematrix);
            //tempmatrix = null;
            min = Integer.MAX_VALUE;
            counter++;
        }
        // print matrix after making the cluster

    }

    public void copyDoubleArray(String[][] arr1, String[][] arr2) {
        for (int i = 0; i < arr1.length; i++) {
            for (int j = 0; j < arr1[i].length; j++) {
                if (arr1[i][j] != null) {
                    arr2[i][j] = arr1[i][j];
                } else {
                    arr2[i][j] = null;
                }
            }
        }
    }

}
