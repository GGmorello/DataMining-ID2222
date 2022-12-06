package se.kth.jabeja;

import org.apache.log4j.Logger;
import se.kth.jabeja.io.CLI;
import se.kth.jabeja.io.GraphReader;
import se.kth.jabeja.config.Config;
import se.kth.jabeja.rand.RandNoGenerator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;


public class Main {
    final static Logger logger = Logger.getLogger(Main.class);

    /**
     * Holds all the configurations parameters.
     */
    private Config config;

    HashMap<Integer, Node> graph;

    public static void main(String[] args) throws IOException {
        new Main().startApp(args);
    }

    private void startApp(String[] args) throws IOException {
        config = (new CLI()).parseArgs(args);

        //set seed for the application
        //Note for the results to be deterministic use
        //only one random generator.
        RandNoGenerator.setSeed(config.getSeed());

        //read the input graph
        HashMap<Integer, Node> graph = readGraph();

        //start JaBeJa
        startJabeja(graph, config.getVersion());
    }

    /**
     * parses the input graph
     *
     * @return
     */
    private HashMap<Integer, Node> readGraph() {
        GraphReader graphReader = new GraphReader();
        graph = graphReader.readGraph(config.getGraphFilePath(), config.getGraphInitialColorPolicy(), config.getNumPartitions());
        return graph;
    }

    /**
     * start the jabeja algorithm
     *
     * @param graph
     * @return
     */
    private void startJabeja(HashMap<Integer, Node> graph, int version) throws IOException {
        if (version == 1) {
            JabejaTask1 host = new JabejaTask1(graph, config);
            host.startJabeja();
        } else {
            JabejaTask2 host = new JabejaTask2(graph, config);
            host.startJabeja();
        }
    }
}
