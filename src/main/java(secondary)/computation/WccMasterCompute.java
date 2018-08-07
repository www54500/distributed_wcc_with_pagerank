package computation;

import aggregators.*;
import computation.comm_initialization.*;
import computation.preprocessing.*;
import computation.wcc_iteration.*;
import computation.seed_expansion.*;
import combiner.TriangleCountCombiner;

import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.block_app.reducers.array.*;
import org.apache.giraph.reducers.impl.*;
import org.apache.giraph.types.ops.IntTypeOps;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.aggregators.LongMinAggregator;
import org.apache.giraph.aggregators.LongMaxAggregator;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleOverwriteAggregator;
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.aggregators.LongOverwriteAggregator;
import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.BooleanOverwriteAggregator;
import org.apache.giraph.aggregators.IntMinAggregator;
import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;

import org.apache.giraph.combiner.SimpleSumMessageCombiner;

import org.apache.giraph.utils.MemoryUtils;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.BooleanWritable;

import java.util.*;

import utils.*;

public class WccMasterCompute extends DefaultMasterCompute {
  /*******************************************************
   *                  AGGREGATOR NAMES                   *
   ******************************************************/
  public static final String GRAPH_CLUSTERING_COEFFICIENT = "graph-clustering-coefficient";
  public static final String MAX_DEGREE = "max-degree";
  public static final String PREPROCESSING_VERTICES_SENT = "preprocessing-vertices-sent";
  public static final String MIN_MEMORY_AVAILABLE = "min-memory-available";

  // Used when a phase is repeated and needs to know how many repetitions have
  // happened so far
  public static final String INTERPHASE_STEP = "interphase-step";

  public static final String COMMUNITY_AGGREGATES = "community-aggregates";

  // Phase control
  public static final String PHASE = "phase";
  public static final String REPEAT_PHASE = "repeat-phase";
  public static final String NEXT_PHASE = "next-phase";
  // True only if a vertex is manually picking next phase
  public static final String PHASE_OVERRIDE = "phase-override"; 
  public static final String NUMBER_OF_PREPROCESSING_STEPS = "number-of-preprocessing-steps";
  public static final String NUMBER_OF_WCC_STEPS = "number-of-wcc-steps";

  // WCC computation
  public static final String WCC = "wcc";
  public static final String BEST_WCC = "best-wcc";

  // Related to termination conditions
  public static final String RETRIES_LEFT = "retries-left";
  public static final String FOUND_NEW_BEST_PARTITION = "found-new-best-partition";
  
  public static final String RATIO_OF_MEMORY = "ratio-of-memory";
  public static final String COMMUNITY_VERTICES_NUMBER = "community-vertices-number";
  public static final String COMMUNITY_VOLUME = "community-volume";
  public static final String PUSH_UPDATE_SENT = "push-update-sent";
  public static final String LIMIT_ROUND = "limit-round";
  public static final String NUMBER_OF_COMMUNITIES = "number-of-communities";
  public static final String COMMUNITY_NEED_MERGE = "community-need-merge";
  public static final String PRIMARY_COMMUNITY_MERGE = "primary-community-merge";
  //public static final String RANDOM_WALK_COMMUNITY_MEMBER = "random-walk-community-member";
  //public static final String ORIGINAL_COMMUNITY_MEMBER = "original-community-member";
  public static final String TOP_COMMUNITIES = "top-communities";
  public static final String SECONDARY_UPDATE = "secondary-update";
  public static final String MAX_AVERAGE_DEGREE = "max-average-degree";
  public static final String CUT = "cut";
  public static final String MAX_VERTEX_ID = "max-vertex-id";
  public static final String PREVIOUS_TOP_COMMUNITIES = "previous-top-communities";
  /*******************************************************
   *                      PHASES                         *
   ******************************************************/
  public static final int INPUT = -1;
  public static final int START = 0;
  public static final int PREPROCESSING_START = 1;
  public static final int SENDING_ADJACENCY_LISTS = 2;
  public static final int COUNTING_TRIANGLES = 3;
  public static final int FINISH_PREPROCESSING = 4;
  public static final int COMMUNITY_INITIALIZATION_START = 5;
  public static final int COMMUNITY_INITIALIZATION_IN_PROCESS = 6;
  public static final int START_WCC_ITERATION = 7;
  public static final int UPDATE_COMMUNITY_INFO = 8;
  public static final int COMPUTE_WCC = 9;
  public static final int CHOOSE_NEXT_COMMUNITY = 10;
  public static final int FIND_COMMUNITY_SEED = 11;
  public static final int RANDOM_WALK_INITIALIZE = 12;
  public static final int RANDOM_WALK = 13;
  public static final int RANDOM_WALK_PUSH = 14;
  public static final int SWEEP_COMMUNITY = 15;
  public static final int SWEEP_COMMUNITY_SORT = 16;
  public static final int SWEEP_COMMUNITY_SCALE = 17;
  public static final int MERGE_COMMUNITIES_INITIALIZATION = 18;
  public static final int MERGE_COMMUNITIES_FIND_TOP = 19;
  public static final int FIND_SECONDARY_COMMUNITIES = 20;
  public static final int MERGE_SECONDARY_COMMUNITIES = 21;
  public static final int COLLECTING_PRIMARY = 22;
  public static final int FIND_PRIMARY_COMMUNITIES = 23;
  public static final int MERGE_PRIMARY_COMMUNITIES = 24;

  /*******************************************************
   *               COMPUTATION-RELATED                   *
   ******************************************************/
  public static final int ISOLATED_COMMUNITY = -1; 
  
  /*******************************************************
   *               CONFIGURATION-RELATED                 *
   ******************************************************/

  public static final String NUMBER_OF_PREPROCESSING_STEPS_CONF_OPT = "wcc.numPreprocessingSteps";
  public static final int DEFAULT_NUMBER_OF_PREPROCESSING_STEPS = 1;

  public static final String NUMBER_OF_WCC_COMPUTATION_STEPS_CONF_OPT = "wcc.numWccComputationSteps";
  public static final int DEFAULT_NUMBER_OF_WCC_COMPUTATION_STEPS = 1;

  // arnau = 7 -> me = 5
  public static final String MAX_RETRIES_CONF_OPT = "wcc.maxRetries";
  public static final int DEFAULT_MAX_RETRIES = 2;
  private static int maxRetries;

  @Override
  public void compute() {
    printMetrics();
    printHeader();

    int retriesLeft = this.<IntWritable>getAggregatedValue(RETRIES_LEFT).get();
    IntWritable phaseAgg = getAggregatedValue(PHASE);
    int previousPhase = (getSuperstep() == 0) ? INPUT : phaseAgg.get(); 
    boolean repeatPhase = this.<BooleanWritable>getAggregatedValue(REPEAT_PHASE).get();
    int nextPhase = getPhaseAfter(previousPhase, repeatPhase); 
    setAggregatedValue(PHASE, new IntWritable(nextPhase));
    //int communityNumber = ((IntWritable) getAggregatedValue("communityNumber")).get();
    
    //parameter setting
    boolean SCDresult = false;
    boolean calConduct = false;
    boolean randomWalkResult = false;
    if (nextPhase == SWEEP_COMMUNITY && !calConduct) {
      nextPhase = MERGE_COMMUNITIES_INITIALIZATION;
      setAggregatedValue(PHASE, new IntWritable(nextPhase));
    }
    MapWritable CommunityAggregator = getAggregatedValue(COMMUNITY_AGGREGATES);
    long maxDegree = ((LongWritable) getAggregatedValue(MAX_DEGREE)).get();
    //double epsilon = (CommunityAggregator.size() == 0 || maxDegree == 0) ? 0 : 1.0 / ((double) CommunityAggregator.size() * (double) maxDegree);
    double epsilon = (maxDegree == 0) ? 0 : 1.0 / ((double) maxDegree);
    //double epsilon = Math.pow(10, -4);
    //double mergeSecondary = 0;
    //double mergePrimary = 0.5;
    //int topCommunity = 354;
    //if (nextPhase == 500 || getSuperstep() == 69) { haltComputation(); }
    if (getSuperstep() == 1) { System.out.printf("epsilon:%f\n", epsilon); }
    
    switch (nextPhase) {
      case START:
        System.out.println("Phase: Start");
        setComputation(StartComputation.class);
        setAggregatedValue(RETRIES_LEFT, new IntWritable(maxRetries));
      break;

      case PREPROCESSING_START:
        System.out.println("Phase: Preprocessing Start");
        setComputation(StartPreprocessingComputation.class);
        System.out.println("Total number of edges before preprocessing: " + getTotalNumEdges()/2.0);
        maxDegree = this.<LongWritable>getAggregatedValue(MAX_DEGREE).get();
        System.out.println("Max degree: " + maxDegree);
      break;

      case SENDING_ADJACENCY_LISTS:
        System.out.println("Phase: Preprocessing (Sending Adjacency Lists)");
        setComputation(SendAdjacencyListComputation.class);
        setMessageCombiner(null);
        if (previousPhase == PREPROCESSING_START) {
          LongWritable preprocessingVerticesSent = this.<LongWritable>getAggregatedValue(PREPROCESSING_VERTICES_SENT);
          System.out.println("Total number of vertices sent in adjacency lists during preprocessing: " + preprocessingVerticesSent);
          int nWorkers = getConf().getMinWorkers();
          double minMemAvailable = this.<DoubleWritable>getAggregatedValue(MIN_MEMORY_AVAILABLE).get();
          double maxMem = minMemAvailable * Math.pow(10, 9); 
          //double maxMem = 10 * Math.pow(10, 9); 
          int numPrepSteps = (int) Math.ceil((double) preprocessingVerticesSent.get() / (nWorkers * maxMem / 8));
          //int numPrepSteps = 5;
          System.out.println("Min workers: " + nWorkers);
          System.out.println("Number of preprocessing steps: " + numPrepSteps);
          setAggregatedValue(NUMBER_OF_PREPROCESSING_STEPS, new IntWritable(numPrepSteps));
          setAggregatedValue(NUMBER_OF_WCC_STEPS, new IntWritable(numPrepSteps));
        } else {
          IntWritable interphaseStepAgg = getAggregatedValue(INTERPHASE_STEP);
          setAggregatedValue(INTERPHASE_STEP, new IntWritable(interphaseStepAgg.get() + 1));
        }
      break;

      case COUNTING_TRIANGLES:
        System.out.println("Phase: Preprocessing (Counting Triangles)");
        setComputation(CountTrianglesComputation.class);
        setMessageCombiner(TriangleCountCombiner.class);
      break;

      case FINISH_PREPROCESSING:
        System.out.println("Phase: Finish Preprocessing");
        setComputation(FinishPreprocessingComputation.class);
      break;

      case COMMUNITY_INITIALIZATION_START:
        System.out.println("Phase: Community Initialization Start");
        System.out.println("Total number of edges after preprocessing: " + getTotalNumEdges()/2.0);
        
        double graphClusteringCoefficient =
            this.<DoubleWritable>getAggregatedValue(GRAPH_CLUSTERING_COEFFICIENT).get();

        System.out.println("Graph clustering coefficient: " + graphClusteringCoefficient / getTotalNumVertices()); 

        setComputation(CommunityInitializationComputation.class);
      break;

      case COMMUNITY_INITIALIZATION_IN_PROCESS:
        System.out.println("Phase: Community Initialization In Process");
      break;

      case START_WCC_ITERATION:
        System.out.println("Phase: WCC Iteration Start");
        setComputation(StartWccIterationComputation.class);
      break;

      case UPDATE_COMMUNITY_INFO:
        System.out.println("Phase: Updating Community Information");
        setComputation(UpdateCommunityInfoComputation.class);
      break;

      case COMPUTE_WCC:
        // Before starting wcc computation again, check for termination
        // If retriesLeft == 0, terminate; otherwise, continue computation
        if (retriesLeft == 0) {
          // TODO: ensure this is the correct WCC for the output results even if
          // it happens on the last one 
          double best_wcc = this.<DoubleWritable>getAggregatedValue(BEST_WCC).get();
          System.out.println("Computation finished.");
          System.out.println("Final WCC: " + best_wcc);
          setComputation(GetSeedInitialComputation.class);
          setAggregatedValue(LIMIT_ROUND, new IntWritable(15));// at least 5 round
          if (SCDresult) { haltComputation(); }
        } else { // continue computation
          System.out.println("Phase: Computing WCC");

          int wccsStepTodo = ((IntWritable) getAggregatedValue(NUMBER_OF_WCC_STEPS)).get();
          // If it's about to be the first phase of wcc computation
          if (previousPhase == UPDATE_COMMUNITY_INFO) {
              setComputation(ComputeWccComputation.class);
              setAggregatedValue(INTERPHASE_STEP, new IntWritable(0));
              System.out.println("interphase step reset");
          } else { // In the middle of iteration
              IntWritable interphaseStepAgg = getAggregatedValue(INTERPHASE_STEP);
              setAggregatedValue(INTERPHASE_STEP, new IntWritable(interphaseStepAgg.get() + 1));
              System.out.println("interphase steps : " + interphaseStepAgg.get() + " / " + wccsStepTodo);
          }
        }
      break;

      case CHOOSE_NEXT_COMMUNITY:
        System.out.println("Phase: Choosing new communities");
        setAggregatedValue(RETRIES_LEFT, new IntWritable(retriesLeft - 1));
        setComputation(ChooseNextCommunityComputation.class);
      break;
        
      case FIND_COMMUNITY_SEED:
        System.out.println("Phase: Find community seed");
        setAggregatedValue(LIMIT_ROUND, new IntWritable(((IntWritable) getAggregatedValue(LIMIT_ROUND)).get() - 1));
        setComputation(GetSeedComputation.class);
      break;
        
      case RANDOM_WALK_INITIALIZE:
        CommunityAggregator = getAggregatedValue(COMMUNITY_AGGREGATES);
        setAggregatedValue(NUMBER_OF_COMMUNITIES, new IntWritable(CommunityAggregator.size()));
        System.out.println("Phase: Random walk initialize");
        System.out.println("Number of community: " + CommunityAggregator.size());
        System.out.println("Epsilon: " + epsilon);
        broadcast("epsilon", new DoubleWritable(epsilon));
        setAggregatedValue(COMMUNITY_VOLUME, new MapWritable()); // reset
        //setAggregatedValue(COMMUNITY_VERTICES_NUMBER, new MapWritable()); // reset
        setComputation(PersonalizedPageRankInitial.class);
      break;
        
      case RANDOM_WALK:
        System.out.println("Phase: Random walk");
        broadcast("epsilon", new DoubleWritable(epsilon));
        LongWritable mapsize = (LongWritable) getAggregatedValue("mapsize");
        System.out.printf("mapsize: %,d\n", mapsize.get());
        setComputation(PersonalizedPageRank.class);
      break;
        
      case RANDOM_WALK_PUSH:
        System.out.println("Phase: Random walk push operation");
        LongWritable push_update_sent = (LongWritable) getAggregatedValue(PUSH_UPDATE_SENT);
        if (push_update_sent.get() != 0) {
          System.out.printf("push-update-sent = %,d\n", push_update_sent.get());
        }
        int nWorkers = getConf().getMinWorkers();
        double minMemAvailable = this.<DoubleWritable>getAggregatedValue(MIN_MEMORY_AVAILABLE).get();
        double maxMem = minMemAvailable * Math.pow(10, 9); 
        //double maxMem = 10 * Math.pow(10, 9); 
        double ratio = (nWorkers * maxMem / 128) / (double) push_update_sent.get();
        //double ratio = 0.5;
        //int numPrepSteps = (int) Math.ceil((double) push-update-sent.get() / (nWorkers * maxMem / 16));
        //int numPrepSteps = 5;
        System.out.println("Min workers: " + nWorkers);
        System.out.printf("Ratio of vertice to usable memory: %,.2f\n", ratio);
        setAggregatedValue(RATIO_OF_MEMORY, new DoubleWritable(ratio));
        broadcast("epsilon", new DoubleWritable(epsilon));
        setComputation(PersonalizedPageRankPush.class);
        setAggregatedValue("mapsize", new LongWritable(0));
      break;
      /*
      case SWEEP_COMMUNITY:
        CommunityAggregator = (MapWritable) getAggregatedValue(COMMUNITY_AGGREGATES);
        //boolean continues = false;
        IntWritable commId = new IntWritable(0);
        
        // take one community to scale
        for (Writable w : CommunityAggregator.keySet()) {
          commId = (IntWritable) w;
          //broadcast("sweepComm", commId);
          setAggregatedValue("sweepComm", commId);
          //continues = true;
          break;
        }
        
        CommunityAggregator.remove(commId);
        System.out.println("Phase: Sweep Community: " + commId.get() + " (Remaining " + CommunityAggregator.size() + ")");
        setComputation(SweepComputation.class);
      break;
        
      case SWEEP_COMMUNITY_SORT:
        // sweep: node id, degree, p value
        System.out.println("Phase: Sweep Community: sort p value and prepare calculating conductance");
        TripleArrayListWritable pairArr = (TripleArrayListWritable) getAggregatedValue("sweep");
        Collections.sort(pairArr, new Comparator<IntegerIntegerDoubleTripleWritable>(){
          @Override
          public int compare(IntegerIntegerDoubleTripleWritable o1, IntegerIntegerDoubleTripleWritable o2) {
            return o2.getThird() - o1.getThird() > 0 ? 1 : (o2.getThird() - o1.getThird() == 0) ? 0 : -1;
            // o2 - o1 -> descending
          }
        });
        for (IntegerIntegerDoubleTripleWritable iidt : pairArr) {
          int nodeid = iidt.getFirst();
          double pvalue = iidt.getThird();
          //System.out.println("after : node id : " + nodeid + " / p value: " + pvalue);
        }
        setComputation(ConductanceComputation.class);
      break;
        
      case SWEEP_COMMUNITY_SCALE:
        // sweep: place, degree, change
        System.out.println("Phase: Sweep Community: scale the random walk community");
        pairArr = (TripleArrayListWritable) getAggregatedValue("sweep");
        Collections.sort(pairArr, new Comparator<IntegerIntegerDoubleTripleWritable>(){
          @Override
          public int compare(IntegerIntegerDoubleTripleWritable o1, IntegerIntegerDoubleTripleWritable o2) {
            return o1.getFirst() - o2.getFirst();
            // o1 - o2 -> ascending
          }
        });
        //calculate conductance
        int curPlace = -1;
        long curCut = 0;
        long curVol = 0;
        long totalDegree = getTotalNumEdges();
        double conductance = 1.0;
        for (IntegerIntegerDoubleTripleWritable iidt : pairArr) {
          curCut += (int) iidt.getThird();
          curVol += iidt.getSecond();
          double curConduct = (curVol == 0 || totalDegree - curVol == 0) ? 1 : (double) curCut / (double) Math.min(curVol, totalDegree - curVol);
          if (curConduct < conductance) {
            conductance = curConduct;
            curPlace = iidt.getFirst();
          }
          //System.out.println(iidt.getFirst() + "'s conductance = " + curCut + " / " + Math.min(curVol, totalDegree - curVol) + " = " + curConduct);
        }
        System.out.println("Minimum Conductance: " + conductance);
        broadcast("finalPlace", new IntWritable(curPlace));
        //broadcast("sweepComm", commId);
        setComputation(ScaleCommunityComputation.class);
      break;
      */
      case MERGE_COMMUNITIES_INITIALIZATION:
        System.out.println("Phase: Initialize merge Communities");
        setComputation(PersonalizedPageRankResult.class);
        if (randomWalkResult) { haltComputation(); }
      break;
      
      case MERGE_COMMUNITIES_FIND_TOP:
        if (previousPhase == MERGE_COMMUNITIES_INITIALIZATION) {
          System.out.println("Phase: Merge Communities (Find Top Communities)");
          // sort communities by size
          MapWritable comVerNum = (MapWritable) getAggregatedValue(COMMUNITY_VERTICES_NUMBER);
          ArrayList<Map.Entry<Writable,Writable>> arrlist = new ArrayList<Map.Entry<Writable,Writable>>(comVerNum.entrySet());
          Collections.sort(arrlist, new Comparator<Map.Entry<Writable,Writable>>() {
            public int compare(Map.Entry<Writable,Writable> o1, Map.Entry<Writable,Writable> o2) {
              int o1value = ((IntWritable) o1.getValue()).get();
              int o2value = ((IntWritable) o2.getValue()).get();
              return o2value - o1value;
            }
          });
          
          int topCommunity = 0;
          // choose the community size <= 3 to a secondary
          for (int i = 0; i < arrlist.size(); i++) {
            Map.Entry<Writable,Writable> com = arrlist.get(i);
            if (((IntWritable) com.getValue()).get() < 4) {
              topCommunity = i;
              break;
            }
          }
          System.out.println("Number of community size <= 3: " + (arrlist.size()-topCommunity));
          
          // find top communities
          ArrayList<Map.Entry<Writable,Writable>> reduceArr = new ArrayList<Map.Entry<Writable,Writable>>();
          reduceArr = new ArrayList<Map.Entry<Writable,Writable>>(arrlist.subList(0, topCommunity));

          MapWritable map = new MapWritable();
          for (Map.Entry<Writable,Writable> e : reduceArr) {
            map.put((IntWritable) e.getKey(), new ArrayPrimitiveWritable(new int[0]));
          }
          setAggregatedValue(TOP_COMMUNITIES, map);
        } else {
          System.out.println("Phase: Merge Communities (Collect Secondary Information)");
          
          // flush TOP_COMMUNITIES value
          MapWritable topComm = (MapWritable) getAggregatedValue(TOP_COMMUNITIES);
          for (Map.Entry<Writable, Writable> e : topComm.entrySet()) {
            topComm.put(e.getKey(), new ArrayPrimitiveWritable(new int[0]));
          }
          setAggregatedValue(TOP_COMMUNITIES, topComm);
        }
        
        setComputation(CollectingOverlapCommunities.class);
      break;
      
      case FIND_SECONDARY_COMMUNITIES:
        System.out.println("Phase: Merge Communities (Find Overlap Secondary Communities)");
        TripleArrayListWritable mergeCommunity = (TripleArrayListWritable) getAggregatedValue(COMMUNITY_NEED_MERGE);
        MapWritable comVerNum = (MapWritable) getAggregatedValue(COMMUNITY_VERTICES_NUMBER);
        MapWritable topComm = (MapWritable) getAggregatedValue(TOP_COMMUNITIES);
        HashMap<Integer, ArrayList> commMap = new HashMap<Integer, ArrayList>();
        
        for (IntegerIntegerDoubleTripleWritable iidt : mergeCommunity){
          int firstComm = iidt.getFirst(); // primary
          int secondComm = iidt.getSecond(); // secondary
          ArrayList<Integer> arr = (commMap.containsKey(firstComm)) ? arr = commMap.get(firstComm) : new ArrayList<Integer>();
          arr.add(secondComm);
          commMap.put(firstComm, arr);
        }
        
        for (Map.Entry<Integer, ArrayList> e : commMap.entrySet()) {
          int comm = e.getKey();
          IntWritable commWritable = new IntWritable(comm);
          ArrayList<Integer> arrlist = e.getValue();
          int[] arr = toPrimitiveArray(arrlist);
          Arrays.sort(arr);
          if (topComm.containsKey(commWritable)) {
            topComm.put(commWritable, new ArrayPrimitiveWritable(arr));
          }
        }
        setAggregatedValue(TOP_COMMUNITIES, topComm);
        setComputation(computeConductance.class);
      break;
        
      case MERGE_SECONDARY_COMMUNITIES:
        System.out.println("Phase: Merge Communities (Merge Secondary Communities via Conductance)");
        topComm = (MapWritable) getAggregatedValue(TOP_COMMUNITIES);
        MapWritable cutMap = (MapWritable) getAggregatedValue(CUT);
        MapWritable volumeMap = (MapWritable) getAggregatedValue(COMMUNITY_VOLUME);
        HashMap<Integer, ArrayList> communityMap = new HashMap<Integer, ArrayList>();
        ArrayList<double[]> needMergeSecond = new ArrayList<double[]>();
        
        // can reset top community map at same time
        for (Map.Entry<Writable, Writable> e : topComm.entrySet()) {
          int name = ((IntWritable) e.getKey()).get();
          communityMap.put(name, new ArrayList<Integer>());
        }
        
        boolean update = false;
        for (Map.Entry<Writable, Writable> e : topComm.entrySet()) {
          int primary = ((IntWritable) e.getKey()).get();
          int[] secondarys = (int[]) ((ArrayPrimitiveWritable) e.getValue()).get();
          if (secondarys.length == 0) { continue; } 
          int primaryCut = ((IntWritable) (cutMap.get(new IntIntPairWritable(primary, primary)))).get();
          int primaryVolume = ((IntWritable) (volumeMap.get(new IntWritable(primary)))).get();
          double orginalConductance = (double) primaryCut / (double) primaryVolume;
          int count = 0;
          for (int secondary : secondarys) {
            int cut = ((IntWritable) (cutMap.get(new IntIntPairWritable(primary, secondary)))).get(); // cut of merged community
            int volume = primaryVolume + ((IntWritable) (volumeMap.get(new IntWritable(secondary)))).get(); // volume of merged community (two volume add)
            double mergedConductance = (double) cut / (double) volume;
            if (mergedConductance <= orginalConductance) { // two communities can merge
              ArrayList arr = communityMap.get(primary);
              arr.add(secondary);
              communityMap.put(primary, arr);
              update = true;
              count++;
            }
          }
        }
        
        for (Map.Entry<Integer, ArrayList> e : communityMap.entrySet()) {
          IntWritable commWritable = new IntWritable(e.getKey());
          ArrayList<Integer> arrlist = e.getValue();
          int[] arr = toPrimitiveArray(arrlist);
          Arrays.sort(arr);
          if (topComm.containsKey(commWritable)) {
            topComm.put(commWritable, new ArrayPrimitiveWritable(arr));
          } else {
            System.out.println("[Error] " + commWritable.get() + " do not contain in map");
            haltComputation();
          }
        }
        
        if (!update) { haltComputation(); }
        else { 
          setAggregatedValue(PHASE_OVERRIDE, new BooleanWritable(true));
          setAggregatedValue(NEXT_PHASE, new IntWritable(MERGE_COMMUNITIES_FIND_TOP));
        }
        
        setAggregatedValue(TOP_COMMUNITIES, topComm);
        //setAggregatedValue(SECONDARY_UPDATE, new BooleanWritable(update));
        setAggregatedValue(CUT, new MapWritable());
        setAggregatedValue(COMMUNITY_VOLUME, new MapWritable());
        setComputation(MergeCommunitiesComputation.class);
      break;
      /*
      case COLLECTING_PRIMARY:
        System.out.println("Phase: Merge Communities (Collect Primary Information)");
        setComputation(CollectingPrimaryCommunities.class);
      break;
        
      case FIND_PRIMARY_COMMUNITIES:
        System.out.println("Phase: Merge Communities (Find Overlap Primary Communities)");
        mergeCommunity = (TripleArrayListWritable) getAggregatedValue(PRIMARY_COMMUNITY_MERGE);
        comVerNum = (MapWritable) getAggregatedValue(COMMUNITY_VERTICES_NUMBER);
        topComm = (MapWritable) getAggregatedValue(TOP_COMMUNITIES);
        commMap = new HashMap<Integer, ArrayList>();
        //ArrayList<Integer> inBox = new ArrayList<Integer>();
        //ArrayList<Integer> remove = new ArrayList<Integer>();
        
        for (Map.Entry<Writable, Writable> e : topComm.entrySet()) {
          int commName = ((IntWritable) e.getKey()).get();
          commMap.put(commName, new ArrayList<Integer>());
        }
        
        int countPrimary = 0;
        for (IntegerIntegerDoubleTripleWritable iidt : mergeCommunity){
          int firstComm = iidt.getFirst();
          int secondComm = iidt.getSecond();
          //int firstCommVertices = ((IntWritable) comVerNum.get(new IntWritable(firstComm))).get();
          //int secondCommVertices = ((IntWritable) comVerNum.get(new IntWritable(secondComm))).get();
          double overlapVertices = iidt.getThird();
          if (overlapVertices > 0) {
            if (firstComm < secondComm) { // small id as the key, large id as the value
              ArrayList<Integer> current = commMap.get(firstComm);
              current.add(secondComm);
              commMap.put(firstComm, current);
            } else {
              ArrayList<Integer> current = commMap.get(secondComm);
              current.add(firstComm);
              commMap.put(secondComm, current);
            }
            countPrimary++;
          }
        }
        //System.out.print("Number of community pairs be merged: " + countPrimary + "\n");
        
        // put the merge result to the mapwritable
        for (Map.Entry<Integer, ArrayList> e : commMap.entrySet()) {
          int comm = e.getKey();
          IntWritable commWritable = new IntWritable(comm);
          ArrayList<Integer> arrlist = e.getValue();
          int[] arr = toPrimitiveArray(arrlist);
          Arrays.sort(arr);
          if (topComm.containsKey(commWritable)) {
            topComm.put(commWritable, new ArrayPrimitiveWritable(arr));
          } else {
            System.out.println("[Error] " + comm + " is not a primary community.");
            haltComputation();
          }
        }
        
        // here need to save the previous topComm, since this topcomm is used to store the primary which may be merged
        setAggregatedValue(PREVIOUS_TOP_COMMUNITIES, (MapWritable) getAggregatedValue(TOP_COMMUNITIES));
        setAggregatedValue(TOP_COMMUNITIES, topComm);
        setComputation(computeConductance.class);
      break;
      
      case MERGE_PRIMARY_COMMUNITIES:
        System.out.println("Phase: Merge Communities (Merge Primary Communities via Conductance)");
        // NOTE! the community named "secondary" below in this case is the primary community, so need to remove from the topcomm
        ArrayList<Integer> remove = new ArrayList<Integer>(); // list to record the merged community
        MapWritable  priamryComm = (MapWritable) getAggregatedValue(TOP_COMMUNITIES); // this map has been used to store the primary may be merged
        topComm = (MapWritable) getAggregatedValue(PREVIOUS_TOP_COMMUNITIES); // this map is the true top community map
        cutMap = (MapWritable) getAggregatedValue(CUT);
        volumeMap = (MapWritable) getAggregatedValue(COMMUNITY_VOLUME);
        HashMap<Integer, double[]> communityMapWithCond = new HashMap<Integer, double[]>(); // a map store one community and whose conductance
        needMergeSecond = new ArrayList<double[]>();
        
        // can reset top community map at same time
        for (Map.Entry<Writable, Writable> e : priamryComm.entrySet()) {
          int name = ((IntWritable) e.getKey()).get();
          communityMapWithCond.put(name, new double[0]);
        }
        
        // compute conductance
        for (Map.Entry<Writable, Writable> e : priamryComm.entrySet()) {
          int primary = ((IntWritable) e.getKey()).get();
          int[] secondarys = (int[]) ((ArrayPrimitiveWritable) e.getValue()).get();
          if (secondarys.length == 0) { continue; } 
          int primaryCut = ((IntWritable) (cutMap.get(new IntIntPairWritable(primary, primary)))).get();
          int primaryVolume = ((IntWritable) (volumeMap.get(new IntWritable(primary)))).get();
          double primaryConductance = (double) primaryCut / (double) primaryVolume;
          int count = 0;
          for (int secondary : secondarys) {
            double lowestConduct = 1; // record the lowest conductance
            int secondaryCut = ((IntWritable) (cutMap.get(new IntIntPairWritable(secondary, secondary)))).get();
            int secondaryVolume = ((IntWritable) (volumeMap.get(new IntWritable(secondary)))).get();
            double secondaryConductance = (double) primaryCut / (double) primaryVolume;
            
            // cut and volume after merged
            int cut = ((IntWritable) (cutMap.get(new IntIntPairWritable(primary, secondary)))).get(); // cut of merged community
            int volume = primaryVolume + secondaryVolume; // volume of merged community (two volume add)
            double mergedConductance = (double) cut / (double) volume;
            if (mergedConductance < primaryConductance && mergedConductance < secondaryConductance) { // two communities can merge
              if (mergedConductance < lowestConduct) { // every primary only merge with the community had lowest conductance 
                communityMapWithCond.put(primary, new double[]{(double) secondary, mergedConductance});
              }
            }
          }
        }
        
        // check whether some primarys want to merge the same secondary
        // If have, leave the one with lower conductance
        ArrayList<Integer> keylist = new ArrayList<Integer>();
        ArrayList<Integer> valuelist = new ArrayList<Integer>();
        for (Map.Entry<Integer, double[]> e : communityMapWithCond.entrySet()) {
          int primary = e.getKey();
          double[] arr = e.getValue();
          if (arr.length == 0) { continue; }
          int secondary = (int) arr[0];
          double conductance = arr[1];
          if (valuelist.contains(new Integer(secondary))) { // two primarys want to merge the same community
            int index = valuelist.indexOf(new Integer(secondary));
            int prePrimary = keylist.get(index); // previous primary
            double[] arr2 = communityMapWithCond.get(prePrimary);
            double preConductance = arr2[1]; // previous conductance
            
            // compare two conductance of community
            if (conductance < preConductance) { // current conductance is smaller
              // remove previous element since its conductance is higher
              communityMapWithCond.put(prePrimary, new double[0]);
              valuelist.set(index, -1); // 
              
              // add current primary and secondary for the next check
              keylist.add(primary);
              valuelist.add(secondary);
            } else {
              // remove current element since its conductance is higher
              communityMapWithCond.put(primary, new double[0]);
            }
          } else {
            keylist.add(primary);
            valuelist.add(secondary);
          }
        }
        
        // put the merge result to the map
        for (Map.Entry<Integer, double[]> e : communityMapWithCond.entrySet()) {
          IntWritable commWritable = new IntWritable(e.getKey());
          double[] arr = e.getValue();
          if (arr.length == 0) { continue; }
          int secondary = (int) arr[0];
          int[] outputarr = new int[]{secondary};
          remove.add(secondary); // record the community merged, it will be removed from topcomm
          
          if (topComm.containsKey(commWritable)) {
            topComm.put(commWritable, new ArrayPrimitiveWritable(outputarr));
          } else {
            System.out.println("[Error] " + commWritable.get() + " do not contain in map");
            haltComputation();
          }
        }
        
        if (remove.size() == 0) { // no primary communities need to merge, go to next round
          setAggregatedValue(PHASE_OVERRIDE, new BooleanWritable(true));
          setAggregatedValue(NEXT_PHASE, new IntWritable(MERGE_COMMUNITIES_FIND_TOP));
          BooleanWritable secondaryUpdate = (BooleanWritable) getAggregatedValue(SECONDARY_UPDATE);
          if (!secondaryUpdate.get()) {
            System.out.println("No communities need to merge, finish.");
            haltComputation();
          }
        } else {
          System.out.println(remove.size()+" communities are merged.");
          for (int i : remove) {
            topComm.remove(new IntWritable(i)); // this community had been merged, remove it
          }
          setAggregatedValue(PHASE_OVERRIDE, new BooleanWritable(true));
          setAggregatedValue(NEXT_PHASE, new IntWritable(COLLECTING_PRIMARY));
        }
        
        setAggregatedValue(TOP_COMMUNITIES, topComm);
        setAggregatedValue(PREVIOUS_TOP_COMMUNITIES, new MapWritable()); // reset
        setAggregatedValue(COMMUNITY_VOLUME, new MapWritable()); // reset
        setComputation(MergeCommunitiesComputation.class);
      break;*/
    }
    // Just finished aggregating latest wcc - must check for improvement
    if (previousPhase == CHOOSE_NEXT_COMMUNITY) {
      double wcc = this.<DoubleWritable>getAggregatedValue(WCC).get();
      double best_wcc = this.<DoubleWritable>getAggregatedValue(BEST_WCC).get();
      double new_wcc = wcc/getTotalNumVertices();

      // First time is just after initialization; no transfers yet
      System.out.println("New WCC: " + new_wcc);
      System.out.println("Best Prior WCC: " + best_wcc);
      System.out.println("Retries left: " + retriesLeft);

      // If wcc has improved
      if (new_wcc - best_wcc > 0.0) { 
        System.out.println("Found new best");
        setAggregatedValue(BEST_WCC, new DoubleWritable(new_wcc));
        setAggregatedValue(FOUND_NEW_BEST_PARTITION, new BooleanWritable(true));

        // If wcc has changed by more than 1%
        if ((new_wcc - best_wcc) / best_wcc > 0.01) { 
          System.out.println("Resetting retries");
          setAggregatedValue(RETRIES_LEFT, new IntWritable(maxRetries));
        }
      }
      System.out.println();
    }
  }

  @Override
  public void initialize() throws InstantiationException, IllegalAccessException {
    // TODO: split
    registerAggregator(COMMUNITY_AGGREGATES, CommunityAggregator.class);
    registerAggregator(WCC, DoubleSumAggregator.class);
    registerAggregator(FOUND_NEW_BEST_PARTITION, BooleanOverwriteAggregator.class);
    registerAggregator(REPEAT_PHASE, BooleanOrAggregator.class);
    registerAggregator(PHASE_OVERRIDE, BooleanOrAggregator.class);
    registerAggregator(NEXT_PHASE, IntOverwriteAggregator.class);
    registerAggregator(PREPROCESSING_VERTICES_SENT, LongSumAggregator.class);
    registerAggregator(MIN_MEMORY_AVAILABLE, DoubleMinAggregator.class);
    registerAggregator("sweep", TripleArrayListAggregator.class);
    registerAggregator(PUSH_UPDATE_SENT, LongSumAggregator.class);
    registerAggregator(RATIO_OF_MEMORY, DoubleOverwriteAggregator.class);
    //registerAggregator(RANDOM_WALK_COMMUNITY_MEMBER, CountCommunityMemberAggregator.class);
    //registerAggregator(ORIGINAL_COMMUNITY_MEMBER, CountCommunityMemberAggregator.class);
    registerAggregator(COMMUNITY_NEED_MERGE, MergeIIDAggregator.class);
    registerAggregator(PRIMARY_COMMUNITY_MERGE, MergeIIDAggregator.class);
    registerAggregator(COMMUNITY_VERTICES_NUMBER, CommunityVerticesNumberAggregator.class);
    registerAggregator(CUT, CommunityCutAggregator.class);
    
    //registerPersistentAggregator(COMMUNITY_AGGREGATES, CommunityAggregator.class);
    registerPersistentAggregator(GRAPH_CLUSTERING_COEFFICIENT, DoubleSumAggregator.class);
    registerPersistentAggregator(MAX_DEGREE, LongMaxAggregator.class);
    registerPersistentAggregator(MAX_VERTEX_ID, LongMaxAggregator.class);
    registerPersistentAggregator(BEST_WCC, DoubleMaxAggregator.class);
    registerPersistentAggregator(RETRIES_LEFT, IntMinAggregator.class);
    registerPersistentAggregator(PHASE, IntMinAggregator.class);
    registerPersistentAggregator(INTERPHASE_STEP, IntMaxAggregator.class);
    registerPersistentAggregator(NUMBER_OF_PREPROCESSING_STEPS, IntMaxAggregator.class);
    registerPersistentAggregator(NUMBER_OF_WCC_STEPS, IntMaxAggregator.class);
    registerPersistentAggregator(NUMBER_OF_COMMUNITIES, IntMinAggregator.class);
    registerPersistentAggregator(LIMIT_ROUND, IntMinAggregator.class);
    registerPersistentAggregator("sweepComm", IntMinAggregator.class);
    registerPersistentAggregator(MAX_AVERAGE_DEGREE, DoubleMaxAggregator.class);
    registerPersistentAggregator(TOP_COMMUNITIES, CommunityVerticesNumberAggregator.class);
    registerPersistentAggregator(PREVIOUS_TOP_COMMUNITIES, CommunityVerticesNumberAggregator.class);
    registerPersistentAggregator(SECONDARY_UPDATE, BooleanOrAggregator.class);
    registerPersistentAggregator("mapsize", LongSumAggregator.class);
    registerPersistentAggregator(COMMUNITY_VOLUME, CommunityVerticesNumberAggregator.class);

    // READ CUSTOM CONFIGURATION PARAMETERS
    String numPreprocessingStepsConf = getConf().get(NUMBER_OF_PREPROCESSING_STEPS_CONF_OPT);
    int numPreprocessingSteps = (numPreprocessingStepsConf!= null) ? 
        Integer.parseInt(numPreprocessingStepsConf.trim()) :
        DEFAULT_NUMBER_OF_PREPROCESSING_STEPS;
    System.out.println("Number of preprocessing steps: " + numPreprocessingSteps);
    setAggregatedValue(NUMBER_OF_PREPROCESSING_STEPS, new IntWritable(numPreprocessingSteps));

    String numWccComputationStepsConf = getConf().get(NUMBER_OF_WCC_COMPUTATION_STEPS_CONF_OPT);
    int numWccComputationSteps = (numWccComputationStepsConf != null) ? 
        Integer.parseInt(numWccComputationStepsConf.trim()) :
        DEFAULT_NUMBER_OF_WCC_COMPUTATION_STEPS;
    System.out.println("Number of WCC computation steps: " + numWccComputationSteps);
    setAggregatedValue(NUMBER_OF_WCC_STEPS, new IntWritable(numWccComputationSteps));

    String maxRetriesConf = getConf().get(MAX_RETRIES_CONF_OPT);
    maxRetries = (maxRetriesConf != null) ? 
        Integer.parseInt(maxRetriesConf.trim()) :
        DEFAULT_MAX_RETRIES;
    System.out.println("Max number of retries: " + maxRetries);
    
    setAggregatedValue(INTERPHASE_STEP, new IntWritable(0));
    setAggregatedValue(NUMBER_OF_PREPROCESSING_STEPS, new IntWritable(0));
    setAggregatedValue(BEST_WCC, new DoubleWritable(0.0));
    setAggregatedValue("push-update-sent", new LongWritable(0));
    setAggregatedValue(MAX_AVERAGE_DEGREE, new DoubleWritable(10.0));
  }

  private int getPhaseAfter(int phase, boolean repeatPhase) {
    BooleanWritable phaseOverride = getAggregatedValue(PHASE_OVERRIDE);
    if (phaseOverride.get()) {
        IntWritable nextPhaseAgg = getAggregatedValue(NEXT_PHASE);
        return nextPhaseAgg.get();
    }
    if (repeatPhase)                    return phase;
    if (phase == CHOOSE_NEXT_COMMUNITY) return UPDATE_COMMUNITY_INFO;
    if (phase == COUNTING_TRIANGLES)    return SENDING_ADJACENCY_LISTS;
    return phase + 1;
  }

  private void printHeader() {
    System.out.println();
    System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
    System.out.println();
    System.out.println("WccMasterCompute: Starting superstep " + getSuperstep() + " (" + printCurrentTime() + ")");
    System.out.println();
  }


  private void printMetrics() {
    System.out.println("---------------------------------------------------");
    System.out.println(MemoryUtils.getRuntimeMemoryStats());
    System.out.println("Master memory used: " + (MemoryUtils.totalMemoryMB() - MemoryUtils.freeMemoryMB()));

    double minWorkerMemAvailable = this.<DoubleWritable>getAggregatedValue(MIN_MEMORY_AVAILABLE).get();
    System.out.println("Minimum worker memory available: " + minWorkerMemAvailable);

    System.out.println("---------------------------------------------------");
    System.out.println();
  }
  
  private String printCurrentTime() {
    java.util.Date current = new java.util.Date();
    //java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("HH:mm:ss,SSS");
    //String t = sdf.format(current) + " ";
    return sdf.format(current);
  }
    
  private int hashCode(int x, int y) {
    int a = Math.max(x, y);
    int b = Math.min(x, y);

    return a*31 + b;
  }
  
  private HashMap<Integer, ArrayList> updateCommMap(HashMap<Integer, ArrayList> map, int one, int two) {
    if (map.containsKey(one)) {
      map.get(one).add(two);
    } else {
      System.out.println("[Error] map not contain community " + one);
      haltComputation();
    }
    
    if (map.containsKey(two)) { // two primary community merge
      ArrayList<Integer> mergeArr = mergeArrayNoDup(map.get(one), map.get(two));
      map.put(one, mergeArr);
    }
    
    return map;
  }
  
  private int[] toPrimitiveArray(ArrayList<Integer> arrList) {
    int[] arr = new int[arrList.size()];
    int i = 0;
    for (int x : arrList) {
      arr[i] = x;
      i++;
    }
    
    return arr;
  }
  
  private MapWritable addToResult(MapWritable result, int one, int two) {
    IntWritable pairOne = new IntWritable(one);
    IntWritable pairTwo = new IntWritable(two);
    if (result.containsKey(pairOne)) {
      int[] arr = (int[]) ((ArrayPrimitiveWritable) result.get(pairOne)).get();
      int[] arrNew = new int[arr.length + 1];
      for (int i = 0; i < arr.length; i++) {
        arrNew[i] = arr[i];
      }
      arrNew[arr.length] = two;
      result.put(pairOne, new ArrayPrimitiveWritable(arrNew));
    } else if (result.containsKey(pairTwo)) {
      int[] arr = (int[]) ((ArrayPrimitiveWritable) result.get(pairTwo)).get();
      int[] arrNew = new int[arr.length + 1];
      for (int i = 0; i < arr.length; i++) {
        arrNew[i] = arr[i];
      }
      arrNew[arr.length] = one;
      result.put(pairTwo, new ArrayPrimitiveWritable(arrNew));
    } else {
      int key;
      int arr[];
      int contain = 0;
      // check every vertice
      for (Map.Entry<Writable, Writable> e : result.entrySet()) {
        key = ((IntWritable) e.getKey()).get();
        arr = (int[]) ((ArrayPrimitiveWritable) e.getValue()).get();
        for (int i : arr) {
          if (i == one) {
            contain += 1;
          } else if (i == two) {
            contain += 2;
          }
        }
        
        /*
        * contain = 0, one and two not contain, keep going
        * contain = 1 or 2, one or two contain, add one or two to array
        * contain = 3, one and two both contain, one and two are exist and quit
        */
        if (contain == 1) {
          int[] arrNew = new int[arr.length + 1];
          for (int i = 0; i < arr.length; i++) {
            arrNew[i] = arr[i];
          }
          arrNew[arr.length] = two;
          result.put(new IntWritable(key), new ArrayPrimitiveWritable(arrNew));
          break;
        } else if (contain == 2) {
          int[] arrNew = new int[arr.length + 1];
          for (int i = 0; i < arr.length; i++) {
            arrNew[i] = arr[i];
          }
          arrNew[arr.length] = one;
          result.put(new IntWritable(key), new ArrayPrimitiveWritable(arrNew));
          break;
        } else if (contain == 3) {
          // do nothing
          break;
        }
      }
      if (contain == 0) { // not exist in the map
        arr = new int[1];
        arr[0] = two;
        result.put(pairOne, new ArrayPrimitiveWritable(arr));
      }
    }
    
    return result;
  }
  
  private ArrayList<Integer> mergeArrayNoDup(ArrayList<Integer> one, ArrayList<Integer> two) {
    one.addAll(two);
    Collections.sort(one);
    for (int i = 1; i < one.size(); i++) {
      if (one.size() > 1 && one.get(i - 1) == one.get(i)) {
          one.remove(i);
          i -= 1;
      }
    }
    
    return one;
  }
}