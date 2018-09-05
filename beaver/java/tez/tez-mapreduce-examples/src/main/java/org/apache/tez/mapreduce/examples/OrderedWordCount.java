/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.mapreduce.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.client.TezSessionStatus;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.examples.helpers.SplitsInClientOptionParser;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.processor.map.MapProcessor;
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;
import org.apache.tez.runtime.api.TezRootInputInitializer;
import org.apache.tez.runtime.library.input.ShuffledMergedInputLegacy;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;

/**
 * An MRR job built on top of word count to return words sorted by
 * their frequency of occurrence.
 *
 * Use -DUSE_TEZ_SESSION=true to run jobs in a session mode.
 * If multiple input/outputs are provided, this job will process each pair
 * as a separate DAG in a sequential manner.
 * Use -DINTER_JOB_SLEEP_INTERVAL=<N> where N is the sleep interval in seconds
 * between the sequential DAGs.
 */
public class OrderedWordCount {

  private static Log LOG = LogFactory.getLog(OrderedWordCount.class);

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,IntWritable, Text> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(result, key);
    }
  }

  /**
   * Shuffle ensures ordering based on count of employees per department
   * hence the final reducer is a no-op and just emits the department name
   * with the employee count per department.
   */
  public static class MyOrderByNoOpReducer
      extends Reducer<IntWritable, Text, Text, IntWritable> {

    public void reduce(IntWritable key, Iterable<Text> values,
        Context context
        ) throws IOException, InterruptedException {
      for (Text word : values) {
        context.write(word, key);
      }
    }
  }

  private static DAG createDAG(FileSystem fs, Configuration conf,
      Map<String, LocalResource> commonLocalResources, Path stagingDir,
      int dagIndex, String inputPath, String outputPath,
      boolean generateSplitsInClient) throws Exception {

    Configuration mapStageConf = new JobConf(conf);
    mapStageConf.set(MRJobConfig.MAP_CLASS_ATTR,
        TokenizerMapper.class.getName());
    mapStageConf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        Text.class.getName());
    mapStageConf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        IntWritable.class.getName());
    mapStageConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR,
        TextInputFormat.class.getName());
    mapStageConf.set(FileInputFormat.INPUT_DIR, inputPath);
    mapStageConf.setBoolean("mapred.mapper.new-api", true);

    InputSplitInfo inputSplitInfo = null;
    if (generateSplitsInClient) {
      inputSplitInfo = MRHelpers.generateInputSplits(mapStageConf, stagingDir);
      mapStageConf.setInt(MRJobConfig.NUM_MAPS, inputSplitInfo.getNumTasks());
    }

    MultiStageMRConfToTezTranslator.translateVertexConfToTez(mapStageConf,
        null);

    Configuration iReduceStageConf = new JobConf(conf);
    iReduceStageConf.setInt(MRJobConfig.NUM_REDUCES, 2); // TODO NEWTEZ - NOT NEEDED NOW???
    iReduceStageConf.set(MRJobConfig.REDUCE_CLASS_ATTR,
        IntSumReducer.class.getName());
    iReduceStageConf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        IntWritable.class.getName());
    iReduceStageConf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        Text.class.getName());
    iReduceStageConf.setBoolean("mapred.mapper.new-api", true);

    MultiStageMRConfToTezTranslator.translateVertexConfToTez(iReduceStageConf,
        mapStageConf);

    Configuration finalReduceConf = new JobConf(conf);
    finalReduceConf.setInt(MRJobConfig.NUM_REDUCES, 1);
    finalReduceConf.set(MRJobConfig.REDUCE_CLASS_ATTR,
        MyOrderByNoOpReducer.class.getName());
    finalReduceConf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        Text.class.getName());
    finalReduceConf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        IntWritable.class.getName());
    finalReduceConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,
        TextOutputFormat.class.getName());
    finalReduceConf.set(FileOutputFormat.OUTDIR, outputPath);
    finalReduceConf.setBoolean("mapred.mapper.new-api", true);

    MultiStageMRConfToTezTranslator.translateVertexConfToTez(finalReduceConf,
        iReduceStageConf);

    MRHelpers.doJobClientMagic(mapStageConf);
    MRHelpers.doJobClientMagic(iReduceStageConf);
    MRHelpers.doJobClientMagic(finalReduceConf);

    List<Vertex> vertices = new ArrayList<Vertex>();

    byte[] mapPayload = MRHelpers.createUserPayloadFromConf(mapStageConf);
    byte[] mapInputPayload =
        MRHelpers.createMRInputPayload(mapPayload, null);
    int numMaps = generateSplitsInClient ? inputSplitInfo.getNumTasks() : -1;
    Vertex mapVertex = new Vertex("initialmap", new ProcessorDescriptor(
        MapProcessor.class.getName()).setUserPayload(mapPayload),
        numMaps, MRHelpers.getMapResource(mapStageConf));
    mapVertex.setJavaOpts(MRHelpers.getMapJavaOpts(mapStageConf));
    if (generateSplitsInClient) {
      mapVertex.setTaskLocationsHint(inputSplitInfo.getTaskLocationHints());
      Map<String, LocalResource> mapLocalResources =
          new HashMap<String, LocalResource>();
      mapLocalResources.putAll(commonLocalResources);
      MRHelpers.updateLocalResourcesForInputSplits(fs, inputSplitInfo,
          mapLocalResources);
      mapVertex.setTaskLocalResources(mapLocalResources);
    } else {
      mapVertex.setTaskLocalResources(commonLocalResources);
    }

    Map<String, String> mapEnv = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRTasks(mapStageConf, mapEnv, true);
    mapVertex.setTaskEnvironment(mapEnv);
    Class<? extends TezRootInputInitializer> initializerClazz = generateSplitsInClient ? null
        : MRInputAMSplitGenerator.class;
    MRHelpers.addMRInput(mapVertex, mapInputPayload, initializerClazz);
    vertices.add(mapVertex);

    Vertex ivertex = new Vertex("intermediate_reducer", new ProcessorDescriptor(
        ReduceProcessor.class.getName()).
        setUserPayload(MRHelpers.createUserPayloadFromConf(iReduceStageConf)),
        2,
        MRHelpers.getReduceResource(iReduceStageConf));
    ivertex.setJavaOpts(MRHelpers.getReduceJavaOpts(iReduceStageConf));
    ivertex.setTaskLocalResources(commonLocalResources);
    Map<String, String> ireduceEnv = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRTasks(iReduceStageConf, ireduceEnv, false);
    ivertex.setTaskEnvironment(ireduceEnv);
    vertices.add(ivertex);

    byte[] finalReducePayload = MRHelpers.createUserPayloadFromConf(finalReduceConf);
    Vertex finalReduceVertex = new Vertex("finalreduce",
        new ProcessorDescriptor(
            ReduceProcessor.class.getName()).setUserPayload(finalReducePayload),
                1, MRHelpers.getReduceResource(finalReduceConf));
    finalReduceVertex.setJavaOpts(
        MRHelpers.getReduceJavaOpts(finalReduceConf));
    finalReduceVertex.setTaskLocalResources(commonLocalResources);
    Map<String, String> reduceEnv = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRTasks(finalReduceConf, reduceEnv, false);
    finalReduceVertex.setTaskEnvironment(reduceEnv);
    MRHelpers.addMROutput(finalReduceVertex, finalReducePayload);
    vertices.add(finalReduceVertex);

    DAG dag = new DAG("OrderedWordCount" + dagIndex);
    for (int i = 0; i < vertices.size(); ++i) {
      dag.addVertex(vertices.get(i));
      if (i != 0) {
        dag.addEdge(new Edge(vertices.get(i-1),
            vertices.get(i), new EdgeProperty(
                DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
                SchedulingType.SEQUENTIAL,
                new OutputDescriptor(
                    OnFileSortedOutput.class.getName()),
                new InputDescriptor(
                    ShuffledMergedInputLegacy.class.getName()))));
      }
    }
    return dag;
  }

  private static void printUsage() {
    System.err.println("Usage: orderedwordcount <in> <out> [-generateSplitsInClient true/<false>]");
    System.err.println("Usage (In Session Mode):"
        + " orderedwordcount <in1> <out1> ... <inN> <outN> [-generateSplitsInClient true/<false>]");
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    boolean generateSplitsInClient = false;

    SplitsInClientOptionParser splitCmdLineParser = new SplitsInClientOptionParser();
    try {
      generateSplitsInClient = splitCmdLineParser.parse(otherArgs, false);
      otherArgs = splitCmdLineParser.getRemainingArgs();
    } catch (ParseException e1) {
      System.err.println("Invalid options");
      printUsage();
      System.exit(2);
    }

    boolean useTezSession = conf.getBoolean("USE_TEZ_SESSION", true);
    long interJobSleepTimeout = conf.getInt("INTER_JOB_SLEEP_INTERVAL", 0)
        * 1000;
    if (((otherArgs.length%2) != 0)
        || (!useTezSession && otherArgs.length != 2)) {
      printUsage();
      System.exit(2);
    }

    List<String> inputPaths = new ArrayList<String>();
    List<String> outputPaths = new ArrayList<String>();

    for (int i = 0; i < otherArgs.length; i+=2) {
      inputPaths.add(otherArgs[i]);
      outputPaths.add(otherArgs[i+1]);
    }

    UserGroupInformation.setConfiguration(conf);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();

    TezConfiguration tezConf = new TezConfiguration(conf);
    TezClient tezClient = new TezClient(tezConf);
    ApplicationId appId = tezClient.createApplication();

    FileSystem fs = FileSystem.get(conf);

    String stagingDirStr = Path.SEPARATOR + "user" + Path.SEPARATOR
        + user + Path.SEPARATOR+ ".staging" + Path.SEPARATOR
        + Path.SEPARATOR + appId.toString();
    Path stagingDir = new Path(stagingDirStr);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
    stagingDir = fs.makeQualified(stagingDir);
    TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);

    tezConf.set(TezConfiguration.TEZ_AM_JAVA_OPTS,
        MRHelpers.getMRAMJavaOpts(conf));

    // No need to add jar containing this class as assumed to be part of
    // the tez jars.

    TezSession tezSession = null;
    AMConfiguration amConfig = new AMConfiguration(null,
        null, tezConf, null);
    if (useTezSession) {
      LOG.info("Creating Tez Session");
      TezSessionConfiguration sessionConfig =
          new TezSessionConfiguration(amConfig, tezConf);
      tezSession = new TezSession("OrderedWordCountSession", appId,
          sessionConfig);
      tezSession.start();
      LOG.info("Created Tez Session");
    }

    DAGStatus dagStatus = null;
    DAGClient dagClient = null;
    String[] vNames = { "initialmap", "intermediate_reducer",
      "finalreduce" };

    Set<StatusGetOpts> statusGetOpts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
    try {
      for (int dagIndex = 1; dagIndex <= inputPaths.size(); ++dagIndex) {
        if (dagIndex != 1
            && interJobSleepTimeout > 0) {
          try {
            LOG.info("Sleeping between jobs, sleepInterval="
                + (interJobSleepTimeout/1000));
            Thread.sleep(interJobSleepTimeout);
          } catch (InterruptedException e) {
            LOG.info("Main thread interrupted. Breaking out of job loop");
            break;
          }
        }

        String inputPath = inputPaths.get(dagIndex-1);
        String outputPath = outputPaths.get(dagIndex-1);

        if (fs.exists(new Path(outputPath))) {
          throw new FileAlreadyExistsException("Output directory "
              + outputPath + " already exists");
        }
        LOG.info("Running OrderedWordCount DAG"
            + ", dagIndex=" + dagIndex
            + ", inputPath=" + inputPath
            + ", outputPath=" + outputPath);

        DAG dag = createDAG(fs, conf, null, stagingDir,
            dagIndex, inputPath, outputPath, generateSplitsInClient);

        if (useTezSession) {
          LOG.info("Waiting for TezSession to get into ready state");
          waitForTezSessionReady(tezSession);
          LOG.info("Submitting DAG to Tez Session, dagIndex=" + dagIndex);
          dagClient = tezSession.submitDAG(dag);
          LOG.info("Submitted DAG to Tez Session, dagIndex=" + dagIndex);
        } else {
          LOG.info("Submitting DAG as a new Tez Application");
          dagClient = tezClient.submitDAGApplication(dag, amConfig);
        }

        while (true) {
          dagStatus = dagClient.getDAGStatus(statusGetOpts);
          if(dagStatus.getState() == DAGStatus.State.RUNNING ||
              dagStatus.getState() == DAGStatus.State.SUCCEEDED ||
              dagStatus.getState() == DAGStatus.State.FAILED ||
              dagStatus.getState() == DAGStatus.State.KILLED ||
              dagStatus.getState() == DAGStatus.State.ERROR) {
            break;
          }
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            // continue;
          }
        }


        while (dagStatus.getState() == DAGStatus.State.RUNNING) {
          try {
            ExampleDriver.printDAGStatus(dagClient, vNames);
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              // continue;
            }
            dagStatus = dagClient.getDAGStatus(statusGetOpts);
          } catch (TezException e) {
            LOG.fatal("Failed to get application progress. Exiting");
            System.exit(-1);
          }
        }
        ExampleDriver.printDAGStatus(dagClient, vNames,
            true, true);
        LOG.info("DAG " + dagIndex + " completed. "
            + "FinalState=" + dagStatus.getState());
      }
    } finally {
      fs.delete(stagingDir, true);
      if (useTezSession) {
        tezSession.stop();
      }
    }

    if (!useTezSession) {
      ExampleDriver.printDAGStatus(dagClient, vNames);
      LOG.info("Application completed. " + "FinalState=" + dagStatus.getState());
      System.exit(dagStatus.getState() == DAGStatus.State.SUCCEEDED ? 0 : 1);
    }
  }

  private static void waitForTezSessionReady(TezSession tezSession)
    throws IOException, TezException {
    while (true) {
      TezSessionStatus status = tezSession.getSessionStatus();
      if (status.equals(TezSessionStatus.SHUTDOWN)) {
        throw new RuntimeException("TezSession has already shutdown");
      }
      if (status.equals(TezSessionStatus.READY)) {
        return;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.info("Interrupted while trying to check session status");
        return;
      }
    }
  }

}
