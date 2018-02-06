package com.github.animeshtrivedi.FileBench;

import org.apache.commons.cli.*;

/**
 * Created by atr on 9/30/16.
 */
public class ParseOptions {
    private Options options;
    private int parallel;
    private String inputDir;
    private String warmUpDir;
    private String factory;
    private int projectivity;
    private int selectivity;

    public ParseOptions(){
        options = new Options();
        this.parallel = 16;
        this.factory = "hdfsread";
        this.inputDir = "/sql/tpcds-sff/store_sales/";
        this.warmUpDir = null;
        options.addOption("h", "help", false, "show help.");
        options.addOption("i", "input", true, "input directory location on an HDFS-compatible fs.");
        options.addOption("w", "warmupinput", true, "same as the winput but for warmup routine.");
        options.addOption("t", "test", true, "test.");
        options.addOption("p", "parallel", true, "parallel instances.");
        options.addOption("P", "projectivity", true, "projectivity on the 0th int column.");
        options.addOption("S", "selection", true, "selectivity on the 0th int column.");
        //options.addOption("EP", "enableProjection", false, "enable projection.");
        //options.addOption("ES", "enableSelection", false, "enable selection.");
    }

    public void show_help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Main", options);
    }
    public void parse(String[] args) {
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                show_help();
                System.exit(0);
            }

            if (cmd.hasOption("p")) {
                this.parallel = Integer.parseInt(cmd.getOptionValue("p").trim());
            }

            if (cmd.hasOption("P")) {
                this.projectivity = Integer.parseInt(cmd.getOptionValue("P").trim());
                JavaUtils.projection = this.projectivity;
                JavaUtils.enableProjection = true;
            }

            if (cmd.hasOption("S")) {
                this.selectivity = Integer.parseInt(cmd.getOptionValue("S").trim());
                JavaUtils.selection = this.selectivity;
                JavaUtils.enableSelection = true;
            }

            if (cmd.hasOption("i")) {
                this.inputDir = cmd.getOptionValue("i").trim();
            }

            if (cmd.hasOption("w")) {
                this.warmUpDir = cmd.getOptionValue("w").trim();
            }

            if (cmd.hasOption("t")) {
                this.factory = cmd.getOptionValue("t").trim();
            }

        } catch (ParseException e) {
            System.err.println("Failed to parse command line properties" + e);
            show_help();
            System.exit(-1);
        }
    }

    final public String getFactory(){
        return this.factory;
    }

    public int getParallel(){
        return this.parallel;
    }

    public String getInputDir(){
        return this.inputDir;
    }

    public String getWarmUpDir(){
        return this.warmUpDir;
    }
}
