/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.app.configuration;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 *
 * @author Mohammad ALSHAER <malshaer at LYNGRO>
 */
public class StormConfiguration {
    private static StormConfiguration singleton;

    private Configuration config;

    private StormConfiguration()
    {
        try
        {
            this.config = new PropertiesConfiguration(
                this.getClass().getResource("/m.storm.properties"));
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

    private static StormConfiguration get()
    {
        if (singleton == null)
            singleton = new StormConfiguration();
        return singleton;
    }

    public static String getString(String key)
    {
        return get().config.getString(key);
    }

    public static Integer getInt(String key)
    {
        return get().config.getInt(key);
    }
}
