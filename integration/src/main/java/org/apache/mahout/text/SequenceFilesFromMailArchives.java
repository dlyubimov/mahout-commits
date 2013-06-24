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
package org.apache.mahout.text;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.utils.email.MailOptions;
import org.apache.mahout.utils.email.MailProcessor;
import org.apache.mahout.utils.io.ChunkedWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts a directory of gzipped mail archives into SequenceFiles of specified
 * chunkSize. This class is similar to {@link SequenceFilesFromDirectory} except
 * it uses block-compressed {@link org.apache.hadoop.io.SequenceFile}s and parses out the subject and
 * body text of each mail message into a separate key/value pair.
 */
public final class SequenceFilesFromMailArchives extends AbstractJob {

  private static final Logger log = LoggerFactory.getLogger(SequenceFilesFromMailArchives.class);

  public void createSequenceFiles(MailOptions options) throws IOException {
    ChunkedWriter writer = new ChunkedWriter(getConf(), options.getChunkSize(), new Path(options.getOutputDir()));
    MailProcessor processor = new MailProcessor(options, options.getPrefix(), writer);
    try {
      if (options.getInput().isDirectory()) {
        PrefixAdditionFilter filter = new PrefixAdditionFilter(processor, writer);
        options.getInput().listFiles(filter);
        log.info("Parsed {} messages from {}", filter.getMessageCount(), options.getInput().getAbsolutePath());
      } else {
        long start = System.currentTimeMillis();
        long cnt = processor.parseMboxLineByLine(options.getInput());
        long finish = System.currentTimeMillis();
        log.info("Parsed {} messages from {} in time: {}", cnt, options.getInput().getAbsolutePath(), finish - start);
      }
    } finally {
      Closeables.close(writer, false);
    }
  }

  public class PrefixAdditionFilter implements FileFilter {
    private final MailProcessor processor;
    private final ChunkedWriter writer;
    private long messageCount;

    public PrefixAdditionFilter(MailProcessor processor, ChunkedWriter writer) {
      this.processor = processor;
      this.writer = writer;
      this.messageCount = 0;
    }

    public long getMessageCount() {
      return messageCount;
    }

    @Override
    public boolean accept(File current) {
      if (current.isDirectory()) {
        log.info("At {}", current.getAbsolutePath());
        PrefixAdditionFilter nested = new PrefixAdditionFilter(
          new MailProcessor(processor.getOptions(), processor.getPrefix()
            + File.separator + current.getName(), writer), writer);
        current.listFiles(nested);
        long dirCount = nested.getMessageCount();
        log.info("Parsed {} messages from directory {}", dirCount, current.getAbsolutePath());
        messageCount += dirCount;
      } else {
        try {
          messageCount += processor.parseMboxLineByLine(current);
        } catch (IOException e) {
          throw new IllegalStateException("Error processing " + current, e);
        }
      }
      return false;
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new SequenceFilesFromMailArchives(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    DefaultOptionBuilder optionBuilder = new DefaultOptionBuilder();
    ArgumentBuilder argumentBuilder = new ArgumentBuilder();

    addInputOption();
    addOutputOption();
    addOption(DefaultOptionCreator.methodOption().create());

    addOption(optionBuilder.withLongName("chunkSize").withArgument(
      argumentBuilder.withName("chunkSize").withMinimum(1).withMaximum(1).create())
      .withDescription("The chunkSize in MegaBytes. Defaults to 64")
      .withShortName("chunk").create());

    addOption(optionBuilder.withLongName("keyPrefix").withArgument(
      argumentBuilder.withName("keyPrefix").withMinimum(1).withMaximum(1).create())
      .withDescription("The prefix to be prepended to the key")
      .withShortName("prefix").create());
    addOption(optionBuilder.withLongName("charset")
      .withRequired(true).withArgument(argumentBuilder.withName("charset")
        .withMinimum(1).withMaximum(1).create()).withDescription(
        "The name of the character encoding of the input files")
      .withShortName("c").create());
    addOption(optionBuilder.withLongName("subject")
      .withRequired(false).withDescription(
        "Include the Mail subject as part of the text.  Default is false")
      .withShortName("s").create());
    addOption(optionBuilder.withLongName("to").withRequired(false)
      .withDescription("Include the to field in the text.  Default is false")
      .withShortName("to").create());
    addOption(optionBuilder.withLongName("from").withRequired(false).withDescription(
      "Include the from field in the text.  Default is false")
      .withShortName("from").create());
    addOption(optionBuilder.withLongName("references")
      .withRequired(false).withDescription(
        "Include the references field in the text.  Default is false")
      .withShortName("refs").create());
    addOption(optionBuilder.withLongName("body").withRequired(false)
      .withDescription("Include the body in the output.  Default is false")
      .withShortName("b").create());
    addOption(optionBuilder.withLongName("stripQuoted")
      .withRequired(false).withDescription(
        "Strip (remove) quoted email text in the body.  Default is false")
      .withShortName("q").create());
    addOption(
      optionBuilder.withLongName("quotedRegex")
        .withRequired(false).withArgument(argumentBuilder.withName("regex")
        .withMinimum(1).withMaximum(1).create()).withDescription(
        "Specify the regex that identifies quoted text.  "
          + "Default is to look for > or | at the beginning of the line.")
        .withShortName("q").create());
    addOption(
      optionBuilder.withLongName("separator")
        .withRequired(false).withArgument(argumentBuilder.withName("separator")
        .withMinimum(1).withMaximum(1).create()).withDescription(
        "The separator to use between metadata items (to, from, etc.).  Default is \\n")
        .withShortName("sep").create());

    addOption(
      optionBuilder.withLongName("bodySeparator")
        .withRequired(false).withArgument(argumentBuilder.withName("bodySeparator")
        .withMinimum(1).withMaximum(1).create()).withDescription(
        "The separator to use between lines in the body.  Default is \\n.  "
          + "Useful to change if you wish to have the message be on one line")
        .withShortName("bodySep").create());
    addOption(DefaultOptionCreator.helpOption());
    Map<String, List<String>> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    File input = getInputFile();
    String outputDir = getOutputPath().toString();

    int chunkSize = 64;
    if (hasOption("chunkSize")) {
      chunkSize = Integer.parseInt(getOption("chunkSize"));
    }

    String prefix = "";
    if (hasOption("keyPrefix")) {
      prefix = getOption("keyPrefix");
    }

    Charset charset = Charset.forName(getOption("charset"));
    MailOptions options = new MailOptions();
    options.setInput(input);
    options.setOutputDir(outputDir);
    options.setPrefix(prefix);
    options.setChunkSize(chunkSize);
    options.setCharset(charset);

    List<Pattern> patterns = Lists.newArrayListWithCapacity(5);
    // patternOrder is used downstream so that we can know what order the text
    // is in instead of encoding it in the string, which
    // would require more processing later to remove it pre feature selection.
    Map<String, Integer> patternOrder = Maps.newHashMap();
    int order = 0;
    if (hasOption("from")) {
      patterns.add(MailProcessor.FROM_PREFIX);
      patternOrder.put(MailOptions.FROM, order++);
    }
    if (hasOption("to")) {
      patterns.add(MailProcessor.TO_PREFIX);
      patternOrder.put(MailOptions.TO, order++);
    }
    if (hasOption("references")) {
      patterns.add(MailProcessor.REFS_PREFIX);
      patternOrder.put(MailOptions.REFS, order++);
    }
    if (hasOption("subject")) {
      patterns.add(MailProcessor.SUBJECT_PREFIX);
      patternOrder.put(MailOptions.SUBJECT, order++);
    }
    options.setStripQuotedText(hasOption("stripQuoted"));

    options.setPatternsToMatch(patterns.toArray(new Pattern[patterns.size()]));
    options.setPatternOrder(patternOrder);
    options.setIncludeBody(hasOption("body"));
    options.setSeparator("\n");
    if (hasOption("separator")) {
      options.setSeparator(getOption("separator"));
    }
    if (hasOption("bodySeparator")) {
      options.setBodySeparator(getOption("bodySeparator"));
    }
    if (hasOption("quotedRegex")) {
      options.setQuotedTextPattern(Pattern.compile(getOption("quotedRegex")));
    }

    if (getOption(DefaultOptionCreator.METHOD_OPTION,
      DefaultOptionCreator.MAPREDUCE_METHOD).equals(DefaultOptionCreator.SEQUENTIAL_METHOD)) {
      runSequential(options);
    } else {
      runMapReduce(getInputPath(), getOutputPath());
    }

    return 0;
  }

  private int runSequential(MailOptions options)
    throws IOException, InterruptedException, NoSuchMethodException {

    long start = System.currentTimeMillis();
    createSequenceFiles(options);
    long finish = System.currentTimeMillis();
    log.info("Conversion took {}ms", finish - start);

    return 0;
  }

  private int runMapReduce(Path input, Path output) throws IOException, InterruptedException, ClassNotFoundException {

    Job job = prepareJob(input, output, MultipleTextFileInputFormat.class, SequenceFilesFromMailArchivesMapper.class,
      Text.class, Text.class, SequenceFileOutputFormat.class, "SequentialFilesFromMailArchives");

    Configuration jobConfig = job.getConfiguration();

    if (hasOption("keyPrefix")) {
      jobConfig.set("prefix", getOption("keyPrefix"));
    }

    int chunkSize = 0;
    if (hasOption("chunkSize")) {
      chunkSize = Integer.parseInt(getOption("chunkSize"));
      jobConfig.set("chunkSize", String.valueOf(chunkSize));
    }

    Charset charset;
    if (hasOption("charset")) {
      charset = Charset.forName(getOption("charset"));
      jobConfig.set("charset", charset.displayName());
    }

    if (hasOption("from")) {
      jobConfig.set("fromOpt", "true");
    }

    if (hasOption("to")) {
      jobConfig.set("toOpt", "true");
    }

    if (hasOption("references")) {
      jobConfig.set("refsOpt", "true");
    }

    if (hasOption("subject")) {
      jobConfig.set("subjectOpt", "true");
    }

    if (hasOption("quotedRegex")) {
      jobConfig.set("quotedRegex", Pattern.compile(getOption("quotedRegex")).toString());
    }

    if (hasOption("separatorOpt")) {
      jobConfig.set("separatorOpt", getOption("separatorOpt"));
    } else {
      jobConfig.set("separatorOpt", "\n");
    }

    if (hasOption("body")) {
      jobConfig.set("bodyOpt", "true");
    } else {
      jobConfig.set("bodyOpt", "false");
    }

    FileSystem fs = FileSystem.get(jobConfig);
    FileStatus fsFileStatus = fs.getFileStatus(inputPath);

    jobConfig.set("baseinputpath", inputPath.toString());
    String inputDirList = HadoopUtil.buildDirList(fs, fsFileStatus);
    FileInputFormat.setInputPaths(job, inputDirList);

    long chunkSizeInBytes = chunkSize * 1024 * 1024;
    // need to set this to a multiple of the block size, or no split happens
    FileInputFormat.setMaxInputSplitSize(job, chunkSizeInBytes);

    // set the max split locations, otherwise we get nasty debug stuff
    jobConfig.set("mapreduce.job.max.split.locations", "1000000");

    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      return -1;
    }
    return 0;
  }
}
