package com.app.bolts;

/**
 * Created by user on 24-04-17.
 */
public class TestClustering {
    public static void main(String[] args) {
        HierarchicalClusteringBolt hc = new HierarchicalClusteringBolt();
        String[] dataset = new String[]{
                "G,326,LYO00198,CRITER,R CHALLEMEL LACOUR,3,,2017-04-19 16:14:50,198,535,903,,2,2017-04-19 16:15:06",
                "V,393,LYO00245,CRITER,AV VIVIANI,3,16 km/h,2017-04-19 16:14:50,245,761,904,,1,2017-04-19 16:15:08",
                "*,,LYO00248,CRITER,R VAILLANT COUTURIER,3,,2017-04-19 16:14:50,248,691,905,,2,2017-04-19 16:15:06",
                "G,676,LYO00250,CRITER,AV FRANCIS DE PRESSENSE,3,,2017-04-19 16:14:50,250,999,906,,2,2017-04-19 16:15:06",
                "G,59,LYO00253,CRITER,R PROFESSEUR BEAUVISAGE,3,,2017-04-19 16:14:50,253,487,907,,2,2017-04-19 16:15:06",
                "V,1214,LYO00265,CRITER,R MARIE-MADELAINE FOURCADE,3,18 km/h,2017-04-19 16:14:50,265,210,908,,2,2017-04-19 16:15:08",
                "V,316,LYO00303,CRITER,AV PAUL SANTY,3,18 km/h,2017-04-19 16:14:50,303,644,909,,2,2017-04-19 16:15:08",
                "V,1094,LYO00304,CRITER,AV PAUL SANTY,3,18 km/h,2017-04-19 16:14:50,304,712,910,,1,2017-04-19 16:15:08",
                "R,867,LYO00712,CRITER,AV DE LA REPUBLIQUE,2,4 km/h,2017-04-19 00:34:46,712,97,911,,2,2017-04-19 00:34:46",
                "O,387,LYO00716,CRITER,AV DE LA REPUBLIQUE,2,12 km/h,2017-04-19 16:14:50,716,269,912,,2,2017-04-19 16:15:08",
                "V,407,LYO00720,CRITER,AV BARTHELEMY BUYER,2,18 km/h,2017-04-19 16:14:50,720,811,913,,2,2017-04-19 16:15:08",
                "V,597,LYO00730,CRITER,R MARIETTON,2,18 km/h,2017-04-19 16:14:50,730,490,914,,1,2017-04-19 16:15:08",
                "*,,LYO00247,CRITER,R VAILLANT COUTURIER,3,,2017-04-19 16:14:50,247,689,915,,1,2017-04-19 16:15:06",
                "*,,LYO00254,CRITER,R PROFESSEUR BEAUVISAGE,3,,2017-04-19 16:14:50,254,486,916,,1,2017-04-19 16:15:06",
                "*,,LYO00284,CRITER,R DU COMMANDANT AYASSE,3,,2017-04-19 16:14:50,284,306,917,,1,2017-04-19 16:15:06",
                "*,,LYO00309,CRITER,BD EDMOND MICHELET,3,,2017-04-19 16:14:50,309,451,918,,1,2017-04-19 16:15:06",
                "V,996,LYO00833,CRITER,AV CHARLES DE GAULLE,2,18 km/h,2017-04-19 16:14:50,833,521,919,,2,2017-04-19 16:15:08",
                "*,,LYO00997,CRITER,R DE LA REPUBLIQUE,2,,2017-04-19 16:14:50,997,2093,920,,1,2017-04-19 16:15:07",
                "*,,LYO01002,CRITER,R JEAN JAURES,2,,2017-04-19 16:14:50,1002,1170,921,,2,2017-04-19 16:15:07",
                "*,,LYO00257,CRITER,R DU VIVIER,2,,2017-04-19 16:14:50,257,671,922,,1,2017-04-19 16:15:06"
        };
        for(String record: dataset) {
            hc.processRecord(record);
            if(hc.clusters!=null && hc.clusters.size()>0) {
                StringBuilder sb = new StringBuilder();
                hc.print(hc.clusters.get(hc.clusters.size() - 1), sb);
                System.out.println(sb.toString());
            }
        }
    }
}
