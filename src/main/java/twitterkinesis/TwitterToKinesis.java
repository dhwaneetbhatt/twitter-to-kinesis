package twitterkinesis;

import java.util.List;

import com.google.common.collect.Lists;

/**
 * @author Dhwaneet Bhatt
 * @since Nov 14, 2016
 */
public class TwitterToKinesis {

  public static void main(String args[]) {
    List<String> hashtags = Lists.newArrayList("twitter", "api", "kinesis");
    ConsumerThread t = new ConsumerThread(hashtags);
    t.init();
    t.start();
  }

}
