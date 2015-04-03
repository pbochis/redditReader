import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.reddit.auth.RedditAuthenticator;
import net.dean.jraw.RedditClient;
import net.dean.jraw.http.NetworkException;
import net.dean.jraw.models.*;
import net.dean.jraw.paginators.UserContributionPaginator;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by Paul on 2/26/2015.
 */
public class Application {

    public static void main(String[] args){
        RedditClient redditClient = RedditAuthenticator.authenticate();
        RedditReader reader = new RedditReader();
        reader.listenForComments(redditClient);
    }

}
