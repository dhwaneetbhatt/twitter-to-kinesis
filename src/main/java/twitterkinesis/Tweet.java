package twitterkinesis;

/**
 * @author Dhwaneet Bhatt
 * @since Nov 14, 2016
 */
public class Tweet {
  private String hashtag;
  private String username;
  private String text;
  private String ts;
  private String location;
  private String sentiment;

  public String getText() {
    return text;
  }

  public String getUsername() {
    return username;
  }

  public String getTs() {
    return ts;
  }

  public String getHashtag() {
    return hashtag;
  }

  public String getLocation() {
    return location;
  }

  public String getSentiment() {
    return sentiment;
  }

  public Tweet(String text, String username, String ts, String hashtag, String location, String sentiment) {
    this.text = text;
    this.username = username;
    this.ts = ts;
    this.hashtag = hashtag;
    this.location = location;
    this.sentiment = sentiment;
  }
}
