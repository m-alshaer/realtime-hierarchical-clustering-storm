/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.app.models;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Mohammad ALSHAER <malshaer at Cognitus>
 */
public class Cluster {

    public String name;
    public List<String> record = new ArrayList<String>();
    public List<List<String>> record1 = new ArrayList<List<String>>();

    public Cluster(String n, List<String> r, List<String> x, List<String> y) {
        this.name = n;
        record = r;
        addrec1(x);
        addrec1(y);

    }

    public void PrintCluster() {
        System.out.print(name + " ");

        for (String value : record) {
            System.out.print(value + " ");
        }
        System.out.println();
    }

    // this method adds the records containing their values into the list
    public void addrec1(List<String> rec) {
        record1.add(rec);
    }

    public void PrintCluster1() {
        for (int i = 0; i < record.size(); i++) {
            if (record.get(i).startsWith("r") && record.get(i + 1).startsWith("r")) {
                for (List<String> sublist : record1) {
                    for (String value : sublist) {
                        System.out.print(value + " ");
                    }
                    System.out.println();
                }
            }
            if (record.get(i).startsWith("c") && record.get(i + 1).startsWith("c")) {
            }

            if (record.get(i).startsWith("r") && record.get(i + 1).startsWith("c")) {
                List<String> sublist = record1.get(i);
                {
                    for (String value : sublist) {
                        System.out.print(value + " ");
                    }
                    System.out.println();
                }

            }

            break;
        }
    }

}
