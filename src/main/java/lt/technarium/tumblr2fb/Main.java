/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lt.technarium.tumblr2fb;

import com.restfb.*;
import com.restfb.FacebookClient.*;
import com.restfb.types.*;
import com.tumblr.jumblr.*;
import com.tumblr.jumblr.types.*;
import java.io.*;
import java.net.URISyntaxException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import net.htmlparser.jericho.*;

/**
 *
 * @author Rokas
 */
public class Main {

    static java.sql.Connection dbConnection;
    static String dbFileName = "";

    // Determines how Tumbler posts are synced:
    static final int TRM_FULLSYNC = 0; // Syncs all of the Tumblr posts sith DB contents    
    static final int TRM_PARTSYNC = 1; // Syncs only newests posts by stopping on the first already existing occurence of Tumblr post in DB
    static int tumblerReadMode;
    
    // Run in read-only mode: no changes in DB or Facebook page are done
    static boolean emulation;
    
    // What to do
    static boolean taskTumblrSync;     // Synchronize Tumblr posts to DB
    static boolean taskFacebookSync;   // Publish posts from DB to Facebook
    
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException, URISyntaxException {

        //<editor-fold defaultstate="collapsed" desc="Read params and initialize">
        log(true, "==================================================");
        log(true, "Running on Heroku!");
        
        // Set defaults here
        tumblerReadMode = TRM_PARTSYNC;
        emulation = true;
        taskTumblrSync = true;
        taskFacebookSync = false;

        for (String s: args) {
            
            switch(s.trim().toUpperCase()) {
                // Sync Tumbler, only fetch new posts
                case "-TP":
                    taskTumblrSync = true;
                    tumblerReadMode = TRM_PARTSYNC;
                    break;
                // Sync Tumbler cycle through all posts
                case "-TF":
                    taskTumblrSync = true;
                    tumblerReadMode = TRM_FULLSYNC;
                    break;
                // Don't sync tumbler
                case "-TN":
                    taskTumblrSync = false;
                    break;             
                // Sync Facebook
                case "-FB":
                    taskFacebookSync = true;
                    break;                    
                // LIVE mode, all actions are real
                case "-GO":
                    emulation = false;
                    break;                    
                // Print usage and exit
                case "-?":
                    printUsage();
                    return;
                default:
                    log(true, "Invalid param: " + s);
                    return; 
            }
        }
        
        if (!taskFacebookSync && !taskTumblrSync) {
            log(true, "Nothing to do, exiting...");
        }


        String workingPath = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParentFile().getPath() + File.separator;
        /*
        
        Only required for SQLite
        
        dbFileName = workingPath + "Tumblr2FB.db";
        dbFileName = "d:\\Dropbox\\Technarium\\Facebook\\Tumblr2FB.db";
        File f = new File(dbFileName);
        if(!f.exists() || f.isDirectory()) { 
            log(true, "DB file ["+dbFileName+"] is missing, exiting!");
            return;
        }
        */
        log(true, "Starting up in: " + workingPath);
        log(true, "Tasks: ");
        log(true, " - Sync Tumblr: " + (taskTumblrSync?(tumblerReadMode==TRM_FULLSYNC?"FULL":"PARTIAL"):"NO"));
        log(true, " - Sync Facebook: " + (taskFacebookSync?"YES":"NO"));
        log(true, " - Mode: " + (emulation?"SIMULATION":"LIVE")); //TODO: Make up my mind.
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc="Connect to DB">
        log(true, "Connecting to database...");
        connectDB();
        //</editor-fold>
        
        //<editor-fold defaultstate="collapsed" desc="Read config from DB">
        log(true, "Reading configuration...");
        String fbAccessToken = getConfigParam("fbAccessToken");
        String fbAppID = getConfigParam("fbAppID");
        String fbAppSecret = getConfigParam("fbAppSecret");
        String tumblrConsumerKey = getConfigParam("tumblrConsumerKey");
        String tumblrConsumerSecret = getConfigParam("tumblrConsumerSecret");
        String tumblrOauthToken = getConfigParam("tumblrOauthToken");
        String tumblrOauthTokenSecret = getConfigParam("tumblrOauthTokenSecret");
        String tumblrBlogName = getConfigParam("tumblrBlogName");
        //</editor-fold>
        
        //<editor-fold defaultstate="collapsed" desc="Sync TUMBLR">
        if (taskTumblrSync) {
            log(true, "Connecting to TUMBLR...");
            // Get secrets here: https://www.tumblr.com/oauth/apps

            JumblrClient tumblr = new JumblrClient(tumblrConsumerKey, tumblrConsumerSecret);
            tumblr.setToken(tumblrOauthToken, tumblrOauthTokenSecret);
            Blog blog = tumblr.blogInfo(tumblrBlogName);

            int tumblrPostCount = blog.getPostCount();
            int expectedBatches = (int)Math.ceil(tumblrPostCount / 20.0);
            log(true, "Total TUMBLR posts: " + tumblrPostCount + ", will loop through " + expectedBatches + " batches.");

            // TUMBLR retrieves posts in small numbers (20 at this moment), so we need to work in batches.
            int lastTumblrPostNum = 0;
            int batchNum = 0;
            int postsInBatch = 1; // Cheat to start the WHILE cycle

            Map<String, String> options = new HashMap<>();
            List<com.tumblr.jumblr.types.Post> posts;

            //String latestTimestamp = latestTumblrPost();
            boolean done = false;

            TextPost textPost;
            Segment segment;
            Element element;
            String imageURL;

            while (postsInBatch > 0) {
                options.put("offset", Integer.toString(lastTumblrPostNum));
                posts = blog.posts(options);
                postsInBatch = posts.size();
                log(true, "Batch num: " + ++batchNum + " of " + expectedBatches + ", " + postsInBatch + " posts.");
                for (com.tumblr.jumblr.types.Post post: posts) {

                    lastTumblrPostNum++;
                    String postType = post.getType();

                    switch (postType) {
                        case "text":

                            // Find first image in post and save it's URL for Facebook
                            textPost = (TextPost)post;
                            segment = new Source(textPost.getBody());
                            element = segment.getFirstElement("img");
                            imageURL = (element != null) ? element.getAttributeValue("src") : null;

                            done = !saveTumblerPost(post.getId().toString(), post.getDateGMT(), post.getTimestamp().intValue(), post.getShortUrl(), postType, post.getState(), null, null, imageURL, null, "blog.technariumas.lt", post.getShortUrl());
                            break;
                        case "quote":

                            QuotePost quotePost = (QuotePost)post;
                            segment = new Source(quotePost.getText());
                            TextExtractor te = new TextExtractor(segment);                        
                            element = segment.getFirstElement("img");
                            imageURL = (element != null) ? element.getAttributeValue("src") : null;

                            done = !saveTumblerPost(post.getId().toString().toString(), post.getDateGMT(), post.getTimestamp().intValue(), post.getShortUrl(), postType, post.getState(), null, "\""+te.toString()+"\"", imageURL, null, null, ((quotePost.getSourceUrl() != null) ? quotePost.getSourceUrl() : post.getShortUrl()));
                            break;
                        case "link":
                            done = !saveTumblerPost(post.getId().toString(), post.getDateGMT(), post.getTimestamp().intValue(), post.getShortUrl(), postType, post.getState(), null, null, null, null, null, post.getShortUrl());
                            break;
                        case "answer":
                            break;
                        case "video":
                            com.tumblr.jumblr.types.VideoPost videoPost = (com.tumblr.jumblr.types.VideoPost)post;
                            done = !saveTumblerPost(post.getId().toString(), post.getDateGMT(), post.getTimestamp().intValue(), post.getShortUrl(), postType, post.getState(), null, null, null, null, null, videoPost.getPermalinkUrl());
                            break;
                        case "audio":
                            com.tumblr.jumblr.types.AudioPost audioPost = (com.tumblr.jumblr.types.AudioPost)post;
                            done = !saveTumblerPost(post.getId().toString(), post.getDateGMT(), post.getTimestamp().intValue(), post.getShortUrl(), postType, post.getState(), null, null, null, null, null, audioPost.getAudioUrl());
                            break;
                        case "photo":
                            break;
                        case "chat":
                            break;
                        default:
                            done = !saveTumblerPost(post.getId().toString(), post.getDateGMT(), post.getTimestamp().intValue(), post.getShortUrl(), postType, post.getState(), null, null, null, null, null, post.getShortUrl());
                            break;
                    }
                    if (tumblerReadMode==TRM_PARTSYNC && done) break;
                }
                if (tumblerReadMode==TRM_PARTSYNC && done) {
                    log(true, "Duplicate found, stopping TUMBLR synchronization.");
                    break;
                }
            }
            
        }
        //</editor-fold>
        
        //<editor-fold defaultstate="collapsed" desc="Sync FACEBOOK">
        if (taskFacebookSync) {
        
            log(true, "Connecting to FACEBOOK...");
            FacebookClient facebookClient = new DefaultFacebookClient(fbAccessToken, Version.VERSION_2_6);
            Page page;


            page = facebookClient.fetchObject("me", Page.class);
            // Check for new access token.
            // TODO: only do this when is it about to expire
            AccessToken newFBAccessToken = new DefaultFacebookClient(Version.VERSION_2_6).obtainExtendedAccessToken(fbAppID, fbAppSecret, fbAccessToken);

            if (newFBAccessToken.getAccessToken().equals(fbAccessToken))
                log(true, "FACEBOOK token unchanged.");
            else {
                setConfigParam("fbAccessToken", newFBAccessToken.getAccessToken());
                log(true, "FACEBOOK token updated.");
            }

            log(true, "FACEBOOK page name: " + page.getName());

            String tumblrID;
            String tumblrDateGMT;
            String tumblrType;
            String facebookID;
            String facebookMessage;
            String facebookImageURL;
            String facebookCaption;
            String facebookURL;

            Statement stmt = dbConnection.createStatement();
            ResultSet rs = stmt.executeQuery("select "
                        + "Tumblr_ID"                
                        + ", Tumblr_DateGMT"
                        + ", Tumblr_Type"
                        + ", FB_Message"
                        + ", FB_ImageURL"                
                        + ", FB_Caption"
                        + ", FB_URL"                        
                        + " from posts where DoPublish = 1 and lower(Tumblr_State) = 'published' and lower(FB_State) = 'unprocessed' order by Tumblr_DateGMT asc");

            int fbCounter = 0;

            while (rs.next()) {

                //if (counter++ > 3) break;

                tumblrID = rs.getString("Tumblr_ID");
                tumblrDateGMT = rs.getString("Tumblr_DateGMT");
                tumblrType = rs.getString("Tumblr_Type");
                facebookMessage = rs.getString("FB_Message");
                facebookImageURL = rs.getString("FB_ImageURL");            
                facebookCaption = rs.getString("FB_Caption");
                facebookURL = rs.getString("FB_URL");   

                log(false, "Publishing TUMBLR post ["+tumblrID+"] to FACEBOOK...");

                List<Parameter> params = new ArrayList<>();

                try {
                    params.clear();                
                    if (facebookImageURL != null && facebookImageURL.trim().length() > 0) params.add(Parameter.with("picture", facebookImageURL));
                    params.add(Parameter.with("link", facebookURL));

                    switch (tumblrType) {
                        case "text":
                            params.add(Parameter.with("caption", facebookCaption));                        
                            break;
                        case "quote":
                            params.add(Parameter.with("message", facebookMessage));
                            break;
                    }
                    
                    if (!emulation) {
                        updateFBPostState(tumblrID, null, "PROCESSING");
                        facebookID = facebookClient.publish("me/feed", FacebookType.class, params.toArray(new Parameter[0])).getId();
                        if (facebookID == null){
                            log(true, "Error!");
                        } else {
                            updateFBPostState(tumblrID, facebookID, "PUBLISHED");
                            log(false, "OK, FB ID: "+facebookID+". Now backdating...");
                            facebookClient.publish( "/"+facebookID, Boolean.class, Parameter.with("backdated_time", tumblrDateGMT));
                            updateFBPostState(tumblrID, facebookID, "BACKDATED");
                            log(true, "OK!");
                        }
                    }
                    fbCounter++;

                } catch (Exception e) {
                    log(true, "Error! " + e.getMessage());
                    // If error is due to publishing too fast, stop it
                    if (e.getMessage().toLowerCase().contains("you’ve been blocked from using it")) break;
                }

                try {
                    java.lang.Thread.sleep(20000);
                } catch (InterruptedException ex) {
                    java.util.logging.Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                }

            }

            log(true, "Published FACEBOOK posts: " + fbCounter + (emulation?" [EMULATED]":" [LIVE]"));
        }
        //</editor-fold>
    }
    
    static int getTumblrPostsCount(String postID) throws SQLException {
        PreparedStatement ps = dbConnection.prepareStatement("select count (*) from posts where Tumblr_ID = ?");        
        ps.setString(1, postID);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) return rs.getInt(1);
        return -1;
    }

    static String latestTumblrPost() throws SQLException {
        PreparedStatement ps = dbConnection.prepareStatement("select max(Tumblr_Timestamp) from posts");
        ResultSet rs = ps.executeQuery();
        if (rs.next()) return rs.getString(1);
        return null;
    }    
    
    /*
    
    OLD, for SQLite
    
    static void connectDB() throws ClassNotFoundException {
        // Load JDBC driver
        Class.forName("org.sqlite.JDBC");
        try {
          dbConnection = DriverManager.getConnection("jdbc:sqlite:"+dbFileName);
        }
        catch(SQLException e) {
          System.err.println(e.getMessage());
        }
    }
    */

    static void connectDB() throws ClassNotFoundException {
        // Load JDBC driver
        Class.forName("org.postgresql.Driver");
        try {
          String dbUrl = System.getenv("JDBC_DATABASE_URL");
          dbConnection = DriverManager.getConnection(dbUrl);
          //dbConnection = DriverManager.getConnection("jdbc:postgresql://ec2-54-225-72-148.compute-1.amazonaws.com:5432/dargq8fa2soe1v?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory");
          
        }
        catch(SQLException e) {
          System.err.println(e.getMessage());
        }
    }
    
    static void closeDB(java.sql.Connection connection) {
        try
        {
          if(connection != null)
            connection.close();
        }
        catch(SQLException e)
        {
          // connection close failed.
          System.err.println(e);
        }
    }
    
    
    // Retrieves configuration parameter from database by name. Returns null if no parameter was found in DB. Case insensitive.
    static String getConfigParam(String paramName) throws SQLException, ClassNotFoundException {
        if (dbConnection == null) connectDB();
        
        PreparedStatement ps = dbConnection.prepareStatement("select paramValue from config where lower(paramName) = ?");
        ps.setString(1, paramName.toLowerCase());
        ResultSet rs = ps.executeQuery();
        if (rs.next()) return rs.getString(1);
        
        return null;
    }

    // Updates configuration parameter in database, or creates a new one if not present yet.
    static void setConfigParam(String paramName, String paramValue) throws SQLException, ClassNotFoundException {
        if (dbConnection == null) connectDB();
        
        // Check if need to update or insert
        Statement stmt = dbConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from config where lower(paramName) = '"+paramName.toLowerCase()+"'");
        
        if (rs.next() && rs.getInt(1) > 0) {
            PreparedStatement ps = dbConnection.prepareStatement("update config set paramValue = ? where lower(paramName) = ?");
            ps.setString(1, paramValue);
            ps.setString(2, paramName.toLowerCase());
            ps.executeUpdate();
        } else {
            PreparedStatement ps = dbConnection.prepareStatement("insert into config (paramName, paramValue) values (?, ?)");
            ps.setString(1, paramName);
            ps.setString(2, paramValue);
            ps.executeUpdate();
        }

    }
    
    static boolean saveTumblerPost(
            String tumblrID
            , String tumblrDateGMT
            , Integer tumblrTimestamp
            , String tumblrURL
            , String tumblrType
            , String tumblrState
            , String facebookID
            , String facebookMessage
            , String facebookImageURL            
            , String facebookName
            , String facebookCaption
            , String facebookURL
    ) throws SQLException {
        
        // For now, only insert new records, no updates yet
        if (getTumblrPostsCount(tumblrID) > 0) {
            log(true, "TUMBLR post already in DB, skipping... [ID: "+tumblrID+"]");
            return false;
        }

        // Prepared statements should work faster for repetetive tasks, but not sure if this is true for local variables in static methods as well
        PreparedStatement psInsert = dbConnection.prepareStatement(
                "insert into posts "
                    + "(Tumblr_DateGMT"
                    + ", Tumblr_Timestamp"                        
                    + ", Tumblr_URL"
                    + ", Tumblr_Type"
                    + ", Tumblr_State"
                    + ", FB_ID"
                    + ", FB_Message"
                    + ", FB_ImageUrl"                        
                    + ", FB_Name"
                    + ", FB_Caption"
                    + ", FB_URL"                        
                    + ", Tumblr_ID"  // A trick, so that params are the same for insert and update
                    + ", FB_State"
                    + ", FB_PublishedTime"
                    + ", DoPublish"
                    + ")"
                    + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'UNPROCESSED', null, 1)"); 

        PreparedStatement psUpdate = dbConnection.prepareStatement(
                "update posts set "
                    + "Tumblr_DateGMT = ?"
                    + ", Tumblr_Timestamp = ?"                        
                    + ", Tumblr_URL = ?"
                    + ", Tumblr_Type = ?"
                    + ", Tumblr_State = ?"
                    + ", FB_ID = ?"
                    + ", FB_Message = ?"
                    + ", FB_ImageUrl = ?"                        
                    + ", FB_Name = ?"
                    + ", FB_Caption = ?"
                    + ", FB_URL = ?"
                    + ", FB_State = 'UPDATED'"
                + " where Tumblr_ID = ?");
        
        PreparedStatement tempPS = psInsert;

        /* // Not yet...
        if (getTumblrPostsCount(tumblrID) == 0) {
            tempPS = psInsert;
            log(true, "Inserting new TUMBLR post to DB... [ID: "+tumblrID+"]");
        } else {
            tempPS = psUpdate;
            log(true, "Updating existing TUMBLR post in DB... [ID: "+tumblrID+"]");
        }
        */
        log(false, "Inserting new TUMBLR post to DB... [ID: "+tumblrID+"]");
        if (!emulation) {
            int paramNumber = 1;
            tempPS.setString(paramNumber++, tumblrDateGMT);
            tempPS.setInt(paramNumber++, tumblrTimestamp);        
            tempPS.setString(paramNumber++, tumblrURL);
            tempPS.setString(paramNumber++, tumblrType);
            tempPS.setString(paramNumber++, tumblrState);
            tempPS.setString(paramNumber++, facebookID);
            tempPS.setString(paramNumber++, facebookMessage);
            tempPS.setString(paramNumber++, facebookImageURL);        
            tempPS.setString(paramNumber++, facebookName);
            tempPS.setString(paramNumber++, facebookCaption);
            tempPS.setString(paramNumber++, facebookURL);
            tempPS.setString(paramNumber++, tumblrID);
            tempPS.executeUpdate();
        }
        log(true, "OK");
        return true;
    }
    
    static void updateFBPostState(String tumblrID, String facebookID, String newState) throws SQLException {
        
        // For now, only insert new records, no updates yet
        if (getTumblrPostsCount(tumblrID) == 0) {
            log(true, "No such TUMBLR post in DB... [ID: "+tumblrID+"]"); // Normaly this should never happen
            return;
        }

        // Prepared statements should work faster for repetetive tasks, but not sure if this is true for local variables in static methods as well

        PreparedStatement psUpdate = dbConnection.prepareStatement(
                "update posts set "
                    + "FB_ID = ?"
                    + ", FB_State = ?"
                    // + ", FB_PublishedTime = datetime('now')" // For SQLite
                    + ", FB_PublishedTime = to_char(CURRENT_TIMESTAMP AT TIME ZONE 'EEST', 'YYYY-MM-DD HH24:MI:SS')" // FOr PostgreSQL
                + " where Tumblr_ID = ?");
        
        psUpdate.setString(1, facebookID);
        psUpdate.setString(2, newState);
        psUpdate.setString(3, tumblrID);        
        psUpdate.executeUpdate();
    }

    static void printUsage() {
        log(true, "Usage: ");
    }
    
    static String timeStamp() {
        SimpleDateFormat date_format = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");
        java.util.Date resultdate = new java.util.Date();
        return date_format.format(resultdate);
    }

    static void log(boolean singleLine, String ... logData) {
        // TODO: normal logging, e.g. log4j
        String result = "";
        for(String s : logData){
            result += s;
        }
        System.out.print(timeStamp() + ": " + result + (singleLine?"\n":""));
    }
        
}
