package se.kth.jabeja;

import org.apache.log4j.Logger;
import se.kth.jabeja.config.Config;
import se.kth.jabeja.config.NodeSelectionPolicy;
import se.kth.jabeja.io.FileIO;
import se.kth.jabeja.rand.RandNoGenerator;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class JabejaTask2 {
  final static Logger logger = Logger.getLogger(JabejaTask2.class);
  private final Config config;
  private final HashMap<Integer, Node> entireGraph;
  private final List<Integer> nodeIds;
  private int numberOfSwaps;
  private int restartAtRound = -1;
  private int round;
  private float alpha;
  private float beta;
  private float T; // 0 to 1
  private final float ORIGINAL_T;
  private float Tmin = (float) 0.00001; // min value for T
  private boolean resultFileCreated = false;

  // -------------------------------------------------------------------
  public JabejaTask2(HashMap<Integer, Node> graph, Config config) {
    this.entireGraph = graph;
    this.nodeIds = new ArrayList(entireGraph.keySet());
    this.round = 0;
    this.numberOfSwaps = 0;
    this.config = config;
    this.T = 1;
    this.ORIGINAL_T = 1;
    // TODO: is alpha necessary or not?
    this.alpha = config.getAlpha();
    this.beta = config.getBeta();
  }

  // -------------------------------------------------------------------
  public void startJabeja() throws IOException {
    for (round = 0; round < config.getRounds(); round++) {
      for (int id : entireGraph.keySet()) {
        sampleAndSwap(id);
      }

      // one cycle for all nodes have completed.
      // reduce the temperature
      saCoolDown();
      report();
    }
  }

  /**
   * Simulated analealing cooling function
   */
  private void saCoolDown() {
    if (T >= Tmin) {
      T *= beta;
    } else {
      T = Tmin;
      if (restartAtRound == -1) {
        logger.info("T arrived at Tmin on round: " + round);
        // wait a bit before restarting the annealing
        // restartAtRound = round + 100;
        // restartAtRound = (int) Math.floor(round * 1.5);
        restartAtRound = (int) Math.floor(round * 2);
        logger.info("Restarting annealing at round: " + restartAtRound);
      } else if (restartAtRound <= round) {
        logger.info("Restarting annealing now: " + round);
        T = ORIGINAL_T;
        restartAtRound = -1;
      }
    }
  }

  /**
   * Sample and swap algorith at node p
   * 
   * @param nodeId
   */
  private void sampleAndSwap(int nodeId) {
    Node partner = null;
    Node nodep = entireGraph.get(nodeId);

    if (config.getNodeSelectionPolicy() == NodeSelectionPolicy.HYBRID
        || config.getNodeSelectionPolicy() == NodeSelectionPolicy.LOCAL) {
      partner = findPartner(nodeId, getNeighbors(nodep));
    }

    if (config.getNodeSelectionPolicy() == NodeSelectionPolicy.HYBRID
        || config.getNodeSelectionPolicy() == NodeSelectionPolicy.RANDOM) {
      // if local policy fails then randomly sample the entire graph
      if (partner == null) {
        partner = findPartner(nodeId, getSample(nodeId));
      }
    }

    if (partner != null) {
      int pcolor = nodep.getColor();
      int qcolor = partner.getColor();
      partner.setColor(pcolor);
      nodep.setColor(qcolor);
      numberOfSwaps++;
    }
  }

  public Node findPartner(int nodeId, Integer[] nodes) {
    Node nodep = entireGraph.get(nodeId);

    Node bestPartner = null;
    double highestBenefit = 0;

    for (Integer node : nodes) {
      Node q = entireGraph.get(node);

      Double oldCost = calculateCost(nodep, q, false);
      Double newCost = calculateCost(nodep, q, true);
      float acceptanceProbability = calculateAcceptanceProbability(oldCost, newCost, highestBenefit);
      float nextRand = RandNoGenerator.nextFloat();
      // we want to make a swap when 3 cases are true:
      // 1. The oldCost is not the same as newCost (avoid duplicate swaps):
      //    - !oldCost.equals(newCost)
      // 2. The acceptance probability is higher than the random number:
      //    - acceptanceProbability > nextRand
      // 3. We make sure to take the highest benefit swap:
      //    - acceptanceProbability > highestBenefit 

      // The last point is important in order to get a optimal solution -
      // since we might have scenarios where we have a clear improvement (newCost is high),
      // but we replace it since the acceptanceProbabiltity happens to be higher
      // than the random value we generate. In such scenarios, we want to make sure
      // that the higher value wins, so we compare the acceptanceProbability > highestBenefit here
      if (acceptanceProbability > nextRand && !oldCost.equals(newCost) && acceptanceProbability > highestBenefit) {
        // logger.info("Acceptance probability: " + oldCost + "; " + newCost + "; " + acceptanceProbability + "; " + nextRand);
        bestPartner = q;
        highestBenefit = acceptanceProbability;
      }
    }

    return bestPartner;
  }

  private Double calculateCost(Node nodep, Node nodeq, boolean swap) {
    int nodepColor = nodep.getColor();
    int nodeqColor = nodeq.getColor();

    Integer degreeNodepp = getDegree(nodep, swap ? nodeqColor : nodepColor);
    Integer degreeNodeqq = getDegree(nodeq, swap ? nodepColor : nodeqColor);

    Double val = Math.pow(Double.valueOf(degreeNodepp), alpha) + Math.pow(Double.valueOf(degreeNodeqq), alpha);
    return val;
  }

  private float calculateAcceptanceProbability(Double oldCost, Double newCost, Double highest) {
    // always swap when newVal is greater than oldVal
    if (newCost > oldCost && newCost > highest) {
      // System.out.println("Swap nodep/nodeq (newVal < oldVal): " + newVal + "; " + oldVal + "; " + T);
      // TODO: make acceptance probability higher the bigger the difference between values?
      return (float) 1.0;
    } else {
      // sometimes make a swap even though newVal is worse than oldVal - to avoid local optimas
      // System.out.println("Dont swap nodep/nodeq (newVal >= oldVal): " + newVal + "; " + oldVal + "; " + T);
      double a = Math.exp((newCost - oldCost) / T);
      return (float) a;
    }
  }

  /**
   * The the degreee on the node based on color
   * 
   * @param node
   * @param colorId
   * @return how many neighbors of the node have color == colorId
   */
  private int getDegree(Node node, int colorId) {
    int degree = 0;
    for (int neighborId : node.getNeighbours()) {
      Node neighbor = entireGraph.get(neighborId);
      if (neighbor.getColor() == colorId) {
        degree++;
      }
    }
    return degree;
  }

  /**
   * Returns a uniformly random sample of the graph
   * 
   * @param currentNodeId
   * @return Returns a uniformly random sample of the graph
   */
  private Integer[] getSample(int currentNodeId) {
    int count = config.getUniformRandomSampleSize();
    int rndId;
    int size = entireGraph.size();
    ArrayList<Integer> rndIds = new ArrayList<Integer>();

    while (true) {
      rndId = nodeIds.get(RandNoGenerator.nextInt(size));
      if (rndId != currentNodeId && !rndIds.contains(rndId)) {
        rndIds.add(rndId);
        count--;
      }

      if (count == 0)
        break;
    }

    Integer[] ids = new Integer[rndIds.size()];
    return rndIds.toArray(ids);
  }

  /**
   * Get random neighbors. The number of random neighbors is controlled using
   * -closeByNeighbors command line argument which can be obtained from the config
   * using {@link Config#getRandomNeighborSampleSize()}
   * 
   * @param node
   * @return
   */
  private Integer[] getNeighbors(Node node) {
    ArrayList<Integer> list = node.getNeighbours();
    int count = config.getRandomNeighborSampleSize();
    int rndId;
    int index;
    int size = list.size();
    ArrayList<Integer> rndIds = new ArrayList<Integer>();

    if (size <= count)
      rndIds.addAll(list);
    else {
      while (true) {
        index = RandNoGenerator.nextInt(size);
        rndId = list.get(index);
        if (!rndIds.contains(rndId)) {
          rndIds.add(rndId);
          count--;
        }

        if (count == 0)
          break;
      }
    }

    Integer[] arr = new Integer[rndIds.size()];
    return rndIds.toArray(arr);
  }

  /**
   * Generate a report which is stored in a file in the output dir.
   *
   * @throws IOException
   */
  private void report() throws IOException {
    int grayLinks = 0;
    int migrations = 0; // number of nodes that have changed the initial color
    int size = entireGraph.size();

    for (int i : entireGraph.keySet()) {
      Node node = entireGraph.get(i);
      int nodeColor = node.getColor();
      ArrayList<Integer> nodeNeighbours = node.getNeighbours();

      if (nodeColor != node.getInitColor()) {
        migrations++;
      }

      if (nodeNeighbours != null) {
        for (int n : nodeNeighbours) {
          Node p = entireGraph.get(n);
          int pColor = p.getColor();

          if (nodeColor != pColor)
            grayLinks++;
        }
      }
    }

    int edgeCut = grayLinks / 2;

    logger.info("round: " + round +
        ", edge cut:" + edgeCut +
        ", swaps: " + numberOfSwaps +
        ", migrations: " + migrations);

    saveToFile(edgeCut, migrations);
  }

  private void saveToFile(int edgeCuts, int migrations) throws IOException {
    String delimiter = "\t\t";
    String outputFilePath;

    // output file name
    File inputFile = new File(config.getGraphFilePath());
    outputFilePath = config.getOutputDir() +
        File.separator +
        inputFile.getName() + "_" +
        "NS" + "_" + config.getNodeSelectionPolicy() + "_" +
        "GICP" + "_" + config.getGraphInitialColorPolicy() + "_" +
        "T" + "_" + config.getTemperature() + "_" +
        "D" + "_" + config.getDelta() + "_" +
        "RNSS" + "_" + config.getRandomNeighborSampleSize() + "_" +
        "URSS" + "_" + config.getUniformRandomSampleSize() + "_" +
        "A" + "_" + config.getAlpha() + "_" +
        "R" + "_" + config.getRounds() + ".txt";

    if (!resultFileCreated) {
      File outputDir = new File(config.getOutputDir());
      if (!outputDir.exists()) {
        if (!outputDir.mkdir()) {
          throw new IOException("Unable to create the output directory");
        }
      }
      // create folder and result file with header
      String header = "# Migration is number of nodes that have changed color.";
      header += "\n\nRound" + delimiter + "Edge-Cut" + delimiter + "Swaps" + delimiter + "Migrations" + delimiter
          + "Skipped" + "\n";
      FileIO.write(header, outputFilePath);
      resultFileCreated = true;
    }

    FileIO.append(round + delimiter + (edgeCuts) + delimiter + numberOfSwaps + delimiter + migrations + "\n",
        outputFilePath);
  }
}
