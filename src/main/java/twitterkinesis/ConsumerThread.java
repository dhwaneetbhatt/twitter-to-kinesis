package twitterkinesis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

/**
 * @author Dhwaneet Bhatt
 * @since Nov 14, 2016
 */
public class ConsumerThread extends Thread {

  private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final byte FILLER_BYTE = Byte.valueOf("1");

  private List<String> hashtags;
  private int fillerLength;

  private Client hosebirdClient;
  private AmazonKinesisFirehoseClient firehoseClient;
  private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
  private Intelligence intelligence;

  public ConsumerThread(List<String> hashtags) {
    this.hashtags = hashtags;
    this.intelligence = new Intelligence();
  }

  public void init() {
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    hosebirdEndpoint.trackTerms(hashtags);

    // TODO: Read these from properties file
    Authentication hosebirdAuth =
      new OAuth1("<consumer-key>", "<consumer-secret>", "<access-token>", "<access-token-secret>");

    ClientBuilder builder = new ClientBuilder()
      .name("Hosebird-Client-01")
      .hosts(hosebirdHosts)
      .authentication(hosebirdAuth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue));

    this.hosebirdClient = builder.build();

    AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
    this.firehoseClient = new AmazonKinesisFirehoseClient(credentials);
    firehoseClient.setRegion(RegionUtils.getRegion("us-east-1"));
  }

  @Override
  public void run() {
    try {
      hosebirdClient.connect();
      while (!hosebirdClient.isDone()) {
        String msg = msgQueue.take();
        JSONObject json = new JSONObject(msg);
        String hashtag = null;
        try {
          JSONArray hashtagsArray = json.getJSONObject("entities").getJSONArray("hashtags");
          if (hashtagsArray.length() > 0) {
            Iterator<Object> iterator = hashtagsArray.iterator();
            while (iterator.hasNext()) {
              JSONObject j = (JSONObject) iterator.next();
              if (hashtags.contains("#" + j.getString("text"))) {
                hashtag = j.getString("text");
                break;
              }
            }
          }
          String ts = SDF.format(new Date(json.getLong("timestamp_ms")));
          String text = json.getString("text");
          String location = null;
          Object locationObj = json.getJSONObject("user").get("location");
          if (locationObj != null) {
            location = locationObj.toString();
          }
          String username = json.getJSONObject("user").getString("name");
          Tweet t = new Tweet(text, username, ts, hashtag, location, intelligence.getSentiment(text));
          String data = MAPPER.writeValueAsString(t) + "\n";
          PutRecordRequest putRecordRequest = new PutRecordRequest();
          putRecordRequest.setDeliveryStreamName("kinesis-stream");
          Record record = createRecord(data);
          putRecordRequest.setRecord(record);
          firehoseClient.putRecord(putRecordRequest);
        } catch (NullPointerException | ArrayIndexOutOfBoundsException | JSONException e) {
          e.printStackTrace();
        }
      }
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
  }

  private Record createRecord(String data) {
    return new Record().withData(ByteBuffer.wrap(data.getBytes()));
  }

}
