/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams;

import com.github.jukkakarvanen.kafka.streams.test.TestInputTopic;
import com.github.jukkakarvanen.kafka.streams.test.TestOutputTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stream processing unit test of {@link WordCountLambdaExample}, using TopologyTestDriver.
 *
 * See {@link WordCountLambdaExample} for further documentation.
 *
 * See {@link WordCountLambdaIntegrationTest} for the End-to-end integration test using an embedded Kafkacluster.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class WordCountLambdaExampleTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, Long> outputTopic;

  @Before
  public void setup() {
    final StreamsBuilder builder = new StreamsBuilder();
    //Create Actual Stream Processing pipeline
    WordCountLambdaExample.createWordCountStream(builder);
    testDriver = new TopologyTestDriver(builder.build(), WordCountLambdaExample.getStreamsConfiguration("localhost:9092"));
    inputTopic = new TestInputTopic<String, String>(testDriver, WordCountLambdaExample.inputTopic, new Serdes.StringSerde(), new Serdes.StringSerde());
    outputTopic = new TestOutputTopic<String, Long>(testDriver, WordCountLambdaExample.outputTopic, new Serdes.StringSerde(), new Serdes.LongSerde());
  }

  @After
  public void tearDown() {
    try {
      testDriver.close();
    } catch (final RuntimeException e) {
      // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when executed in Windows, ignoring it
      // Logged stacktrace cannot be avoided
      System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
    }
  }


  /**
   *  Simple test validating count of one word
   */
  @Test
  public void testOneWord() {
    //Feed word "Hello" to inputTopic and no kafka key, timestamp is irrelevant in this case
    inputTopic.pipeInput("Hello");
    //Read and validate output
    final ProducerRecord<String, Long> output = outputTopic.readRecord();
    OutputVerifier.compareKeyValue(output, "hello", 1L);
    //No more output in topic
    assertThat(outputTopic.readRecord()).isNull();
  }

  /**
   *  Test Word count of sentence list.
   *  Test logic copied from {@link WordCountLambdaIntegrationTest}
   */
  @Test
  public void shouldCountWords() {
    final List<String> inputValues = Arrays.asList(
        "Hello Kafka Streams",
        "All streams lead to Kafka",
        "Join Kafka Summit",
        "И теперь пошли русские слова"
    );
    final Map<String, Long> expectedWordCounts = new HashMap<>();
    expectedWordCounts.put("hello", 1L);
    expectedWordCounts.put("all", 1L);
    expectedWordCounts.put("streams", 2L);
    expectedWordCounts.put("lead", 1L);
    expectedWordCounts.put("to", 1L);
    expectedWordCounts.put("join", 1L);
    expectedWordCounts.put("kafka", 3L);
    expectedWordCounts.put("summit", 1L);
    expectedWordCounts.put("и", 1L);
    expectedWordCounts.put("теперь", 1L);
    expectedWordCounts.put("пошли", 1L);
    expectedWordCounts.put("русские", 1L);
    expectedWordCounts.put("слова", 1L);

    inputTopic.pipeValueList(inputValues);

    final Map<String, Long> actualWordCounts = outputTopic.readKeyValuesToMap();
    assertThat(actualWordCounts).containsAllEntriesOf(expectedWordCounts).hasSameSizeAs(expectedWordCounts);
  }

}
