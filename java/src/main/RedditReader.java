import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.reddit.auth.RedditAuthenticator;
import com.reddit.storage.DatabaseHelper;
import net.dean.jraw.RedditClient;
import net.dean.jraw.http.NetworkException;
import net.dean.jraw.models.*;
import net.dean.jraw.paginators.UserContributionPaginator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by Paul on 3/24/2015.
 */
public class RedditReader {

    private DatabaseHelper dbHelper;

    public RedditReader(){
        dbHelper = new DatabaseHelper();
        initLangDetector();
    }

    private HttpURLConnection initConnection(){
//        url = new URL("https://reddit.com/comments.json");
        try {
            URL url = new URL("http://dev.fizzlefoo.com/comments.php");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("User-Agent", RedditAuthenticator.myUserAgent.toString());

            int responseCode = connection.getResponseCode();
            if (responseCode == 200){
                return connection;
            }
            return null;
        }
        catch (IOException e){
            e.printStackTrace();
            return null;
        }
    }

    private JSONObject readCommentFromStream(BufferedReader in){
        try {
            String eventId = in.readLine();
            if (eventId == null){
                return null;
            }
            String eventType = in.readLine();
            String eventData = in.readLine();
            in.readLine();  //blank space
            return new JSONObject("{ " + eventId + ", " + eventType + ", " + eventData + "}");
        }
        catch (IOException e){
            e.printStackTrace();
            return null;
        }

    }

    public void listenForComments(RedditClient client){
        try {
            HttpURLConnection connection = initConnection();
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(connection.getInputStream()));

            JSONObject comment = readCommentFromStream(in);

            while (comment != null) {

                String userName = (String)comment.getJSONObject("data").get("author");
                String text = comment.getJSONObject("data").getString("body");
                String id = comment.get("id").toString();

                boolean newUser = dbHelper.saveOrModifyStatusForUser(userName);
                if(isEnglish(text)) {
                    dbHelper.saveComment(id, userName, text);
                }
                getPostsForUser(client, userName);
                if (newUser){
                    getCommentsForUser(client, userName);
                }else {
                    System.out.println("Saved new comment from RECURRING user " + userName);
                }
                comment = readCommentFromStream(in);
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void getCommentsForUser(RedditClient client, String userName) {
        try {
            int count = 0;
            UserContributionPaginator contributionPaginator = new UserContributionPaginator(client, "comments", userName);
            while (contributionPaginator.hasNext()) {
                Listing<Contribution> posts = contributionPaginator.next();
                for (Contribution contribution : posts) {
                    Comment comment = (Comment) contribution;
                    if (isEnglish(comment.getBody())) {
                        dbHelper.saveComment(comment.getId(), userName, comment.getBody());
                        count++;
                    }
                }
            }
            System.out.println("Saved " + count + "comments from " + userName);
        }
        catch (NetworkException e){
            System.out.println("There was a problem counting comments from " + userName);
        }
    }
    private void getPostsForUser(RedditClient client, String userName){
        try{
            int count = 0;
            UserContributionPaginator contributionPaginator = new UserContributionPaginator(client, "submitted", userName);
            while (contributionPaginator.hasNext()) {
                Listing<Contribution> posts = contributionPaginator.next();
                for (Contribution contribution : posts) {
                    PublicContribution c = (PublicContribution)contribution;
                    Submission post = (Submission) contribution;
                    if (isEnglish(post.getSelftext())) {
                        dbHelper.savePost(post.getId(), userName, post.getSelftext());
                        count++;
                    }
                }
            }
            System.out.println("Saved " + count + "posts from " + userName);
        }
        catch (NetworkException e){
            System.out.println("There was a problem grabbing posts from " + userName);
        }

    }

    private void initLangDetector(){
        try{
            DetectorFactory.loadProfile("C:/sdk/languageDetector/profiles");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private boolean isEnglish(String text){
        UrlValidator validator = UrlValidator.getInstance();
        if (validator.isValid(text)){
            return false;
        }
        if (StringUtils.isBlank(text)){
            return false;
        }
        try{
            Detector detector = DetectorFactory.create();
            detector.append(text);
            String lang = detector.detect();
            return lang.toLowerCase().equals("en");
        }
        catch (Exception e){
            System.out.println("Tried to detect for: " + text);
            System.err.println(e.getMessage());

            return false;
        }
    }


}
