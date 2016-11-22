package twitterkinesis;

import ai.api.AIConfiguration;
import ai.api.AIDataService;
import ai.api.model.AIRequest;
import ai.api.model.AIResponse;

/**
 * @author Priyank Doshi
 * @since Nov 14, 2016
 */
public class Intelligence {

  AIConfiguration configuration = new AIConfiguration("<access-token>");
  AIDataService dataService = new AIDataService(configuration);

  public String getSentiment(String tweet) {
    String sentiment = null;
    try {
      AIRequest request = new AIRequest(tweet);
      AIResponse response = dataService.request(request);
      if (response.getStatus().getCode() == 200) {
        sentiment = response.getResult().getAction();
      } else {
        System.err.println(response.getStatus().getErrorDetails());
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return sentiment;
  }
}

