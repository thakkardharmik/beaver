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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.common.TezJobConfig;
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
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.examples.helpers.SplitsInClientOptionParser;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.processor.FilterByWordInputProcessor;
import org.apache.tez.processor.FilterByWordOutputProcessor;
import org.apache.tez.runtime.api.TezRootInputInitializer;
import org.apache.tez.runtime.library.input.ShuffledUnorderedKVInput;
import org.apache.tez.runtime.library.output.OnFileUnorderedKVOutput;

public class FilterLinesByWord {

  private static Log LOG = LogFactory.getLog(FilterLinesByWord.class);

  public static final String FILTER_PARAM_NAME = "tez.runtime.examples.filterbyword.word";

  private static void printUsage() {
    System.err.println("Usage filtelinesrbyword <in> <out> <filter_word> [-generateSplitsInClient true/<false>]");
  }

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, TezException {
    Configuration conf = new Configuration();
    String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

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

    if (otherArgs.length != 3) {
      printUsage();
      System.exit(2);
    }

    String inputPath = otherArgs[0];
    String outputPath = otherArgs[1];
    String filterWord = otherArgs[2];

    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(new Path(outputPath))) {
      System.err.println("Output directory : " + outputPath + " already exists");
      System.exit(2);
    }

    TezConfiguration tezConf = new TezConfiguration(conf);

    fs.getWorkingDirectory();
    Path stagingDir = new Path(fs.getWorkingDirectory(), UUID.randomUUID().toString());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir.toString());
    TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);

    tezConf.set(TezConfiguration.TEZ_AM_JAVA_OPTS, MRHelpers.getMRAMJavaOpts(tezConf));

    String jarPath = ClassUtil.findContainingJar(FilterLinesByWord.class);
    if (jarPath == null) {
      throw new TezUncheckedException("Could not find any jar containing"
          + FilterLinesByWord.class.getName() + " in the classpath");
    }

    Path remoteJarPath = fs.makeQualified(new Path(stagingDir, "dag_job.jar"));
    fs.copyFromLocalFile(new Path(jarPath), remoteJarPath);
    FileStatus remoteJarStatus = fs.getFileStatus(remoteJarPath);

    Map<String, LocalResource> commonLocalResources = new TreeMap<String, LocalResource>();
    LocalResource dagJarLocalRsrc = LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromPath(remoteJarPath),
        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
        remoteJarStatus.getLen(), remoteJarStatus.getModificationTime());
    commonLocalResources.put("dag_job.jar", dagJarLocalRsrc);



    AMConfiguration amConf = new AMConfiguration(null, commonLocalResources, tezConf, null);
    TezSessionConfiguration sessionConf = new TezSessionConfiguration(amConf, tezConf);
    TezSession tezSession = new TezSession("FilterLinesByWordSession", sessionConf);
    tezSession.start(); // Why do I need to start the TezSession.

    Configuration stage1Conf = new JobConf(conf);
    stage1Conf.set(FileInputFormat.INPUT_DIR, inputPath);
    stage1Conf.setBoolean("mapred.mapper.new-api", false);
    stage1Conf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_CLASS, Text.class.getName());
    stage1Conf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_VALUE_CLASS, TextLongPair.class.getName());
    stage1Conf.set(FILTER_PARAM_NAME, filterWord);

    InputSplitInfo inputSplitInfo = null;
    if (generateSplitsInClient) {
      inputSplitInfo = MRHelpers.generateInputSplits(stage1Conf, stagingDir);
    }
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(stage1Conf, null);



    Configuration stage2Conf = new JobConf(conf);
    stage2Conf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_CLASS, Text.class.getName());
    stage2Conf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_VALUE_CLASS, TextLongPair.class.getName());
    stage2Conf.set(FileOutputFormat.OUTDIR, outputPath);
    stage2Conf.setBoolean("mapred.mapper.new-api", false);
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(stage2Conf, stage1Conf);

    MRHelpers.doJobClientMagic(stage1Conf);
    MRHelpers.doJobClientMagic(stage2Conf);

    byte[] stage1Payload = MRHelpers.createUserPayloadFromConf(stage1Conf);
    // Setup stage1 Vertex
    int stage1NumTasks = generateSplitsInClient ? inputSplitInfo.getNumTasks() : -1;
    Vertex stage1Vertex = new Vertex("stage1", new ProcessorDescriptor(
        FilterByWordInputProcessor.class.getName()).setUserPayload(stage1Payload),
        stage1NumTasks, MRHelpers.getMapResource(stage1Conf));
    stage1Vertex.setJavaOpts(MRHelpers.getMapJavaOpts(stage1Conf));
    if (generateSplitsInClient) {
      stage1Vertex.setTaskLocationsHint(inputSplitInfo.getTaskLocationHints());
      Map<String, LocalResource> stage1LocalResources = new HashMap<String, LocalResource>();
      stage1LocalResources.putAll(commonLocalResources);
      MRHelpers.updateLocalResourcesForInputSplits(fs, inputSplitInfo, stage1LocalResources);
      stage1Vertex.setTaskLocalResources(stage1LocalResources);
    } else {
      stage1Vertex.setTaskLocalResources(commonLocalResources);
    }
    Map<String, String> stage1Env = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRTasks(stage1Conf, stage1Env, true);
    stage1Vertex.setTaskEnvironment(stage1Env);

    // Configure the Input for stage1
    Class<? extends TezRootInputInitializer> initializerClazz = generateSplitsInClient ? null
        : MRInputAMSplitGenerator.class;
    stage1Vertex.addInput("MRInput",
        new InputDescriptor(MRInputLegacy.class.getName())
            .setUserPayload(MRHelpers.createMRInputPayload(stage1Payload, null)),
        initializerClazz);

    // Setup stage2 Vertex
    Vertex stage2Vertex = new Vertex("stage2", new ProcessorDescriptor(
        FilterByWordOutputProcessor.class.getName()).setUserPayload(MRHelpers
        .createUserPayloadFromConf(stage2Conf)), 1,
        MRHelpers.getMapResource(stage2Conf));
    stage2Vertex.setJavaOpts(MRHelpers.getReduceJavaOpts(stage2Conf)).setTaskLocalResources(commonLocalResources);
    Map<String, String> stage2Env = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRTasks(stage2Conf, stage2Env, false);
    stage2Vertex.setTaskEnvironment(stage2Env);

    // Configure the Output for stage2
    stage2Vertex.addOutput("MROutput",
        new OutputDescriptor(MROutput.class.getName()).setUserPayload(MRHelpers
            .createUserPayloadFromConf(stage2Conf)));

    DAG dag = new DAG("FilterLinesByWord");
    Edge edge = new Edge(stage1Vertex, stage2Vertex, new EdgeProperty(
        DataMovementType.BROADCAST, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, new OutputDescriptor(
            OnFileUnorderedKVOutput.class.getName()), new InputDescriptor(
            ShuffledUnorderedKVInput.class.getName())));
    dag.addVertex(stage1Vertex).addVertex(stage2Vertex).addEdge(edge);

    LOG.info("Submitting DAG to Tez Session");
    DAGClient dagClient = tezSession.submitDAG(dag);
    LOG.info("Submitted DAG to Tez Session");

    DAGStatus dagStatus = null;
    String[] vNames = { "stage1", "stage2" };
    try {
      while (true) {
        dagStatus = dagClient.getDAGStatus(null);
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
          dagStatus = dagClient.getDAGStatus(null);
        } catch (TezException e) {
          LOG.fatal("Failed to get application progress. Exiting");
          System.exit(-1);
        }
      }
    } finally {
      fs.delete(stagingDir, true);
      tezSession.stop();
    }

    ExampleDriver.printDAGStatus(dagClient, vNames);
    LOG.info("Application completed. " + "FinalState=" + dagStatus.getState());
    System.exit(dagStatus.getState() == DAGStatus.State.SUCCEEDED ? 0 : 1);
  }

  public static class TextLongPair implements Writable {

    private Text text;
    private LongWritable longWritable;

    public TextLongPair() {
    }

    public TextLongPair(Text text, LongWritable longWritable) {
      this.text = text;
      this.longWritable = longWritable;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      this.text.write(out);
      this.longWritable.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.text = new Text();
      this.longWritable = new LongWritable();
      text.readFields(in);
      longWritable.readFields(in);
    }

    @Override
    public String toString() {
      return text.toString() + "\t" + longWritable.get();
    }
  }
}
