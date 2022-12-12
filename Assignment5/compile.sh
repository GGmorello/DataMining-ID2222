#!/bin/bash
mvn clean install
./run.sh -graph ./graphs/twitter.graph
./plot.sh output/twitter.graph_NS_HYBRID_GICP_ROUND_ROBIN_T_2.0_D_0.003_RNSS_3_URSS_6_A_2.0_R_1000.txt