/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.classifier.naivebayes.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.ClassifierResult;
import org.apache.mahout.classifier.ResultAnalyzer;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Test the (Complementary) Naive Bayes model that was built during training
 * by running the iterating the test set and comparing it to the model
 */
public class TestNaiveBayesDriver extends AbstractJob {

  private static final Logger log = LoggerFactory.getLogger(TestNaiveBayesDriver.class);

  public static final String LABEL_KEY = "labels";
  public static final String COMPLEMENTARY = "class"; //b for bayes, c for complementary

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new TestNaiveBayesDriver(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(addOption(DefaultOptionCreator.overwriteOption().create()));
    addOption("model", "m", "The path to the model built during training", true);
    addOption(buildOption("testComplementary", "c", "test complementary?", false, false, String.valueOf(false)));
    addOption("labelIndex", "l", "The path to the location of the label index", true);
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
      HadoopUtil.delete(getConf(), getOutputPath());
    }
    Path model = new Path(parsedArgs.get("--model"));
    HadoopUtil.cacheFiles(model, getConf());
    //the output key is the expected value, the output value are the scores for all the labels
    Job testJob = prepareJob(getInputPath(), getOutputPath(), SequenceFileInputFormat.class, BayesTestMapper.class,
            Text.class, VectorWritable.class, SequenceFileOutputFormat.class);
    //testJob.getConfiguration().set(LABEL_KEY, parsedArgs.get("--labels"));
    boolean complementary = parsedArgs.containsKey("--testComplementary");
    testJob.getConfiguration().set(COMPLEMENTARY, String.valueOf(complementary));
    boolean succeeded = testJob.waitForCompletion(true);
    if (!succeeded) {
      return -1;
    }
    //load the labels
    Map<Integer, String> labelMap = BayesUtils.readLabelIndex(getConf(), new Path(parsedArgs.get("--labelIndex")));

    //loop over the results and create the confusion matrix
    SequenceFileDirIterable<Text, VectorWritable> dirIterable =
        new SequenceFileDirIterable<Text, VectorWritable>(getOutputPath(),
                                                          PathType.LIST,
                                                          PathFilters.partFilter(),
                                                          getConf());
    ResultAnalyzer analyzer = new ResultAnalyzer(labelMap.values(), "DEFAULT");
    analyzeResults(labelMap, dirIterable, analyzer);

    log.info("{} Results: {}", complementary ? "Complementary" : "Standard NB", analyzer);
    return 0;
  }

  private static void analyzeResults(Map<Integer, String> labelMap,
                                     SequenceFileDirIterable<Text, VectorWritable> dirIterable,
                                     ResultAnalyzer analyzer) {
    for (Pair<Text, VectorWritable> pair : dirIterable) {
      int bestIdx = Integer.MIN_VALUE;
      double bestScore = Long.MIN_VALUE;
      for (Vector.Element element : pair.getSecond().get()) {
        if (element.get() > bestScore) {
          bestScore = element.get();
          bestIdx = element.index();
        }
      }
      if (bestIdx != Integer.MIN_VALUE) {
        ClassifierResult classifierResult = new ClassifierResult(labelMap.get(bestIdx), bestScore);
        analyzer.addInstance(pair.getFirst().toString(), classifierResult);
      }
    }
  }
}
