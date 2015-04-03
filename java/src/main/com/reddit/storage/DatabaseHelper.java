package com.reddit.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.*;
import com.marklogic.client.io.*;
import com.marklogic.client.io.marker.JSONWriteHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;

/**
 * Created by Paul on 3/17/2015.
 */
public class DatabaseHelper {

    //collection names

    private final String COMMENT = "comment";
    private final String SUBMISSION = "submission";


    private class DbCollections {
        protected static final String USER_COLLECTION = "reddit_users";
        protected static final String SUBMISSION_COLLECTION = "reddit_submissions";
    }


    private class JsonProperties{
        protected class User{
            protected static final String USER_NAME = "user_name";
            protected static final String INFORMATION_PARSED = "information_parsed";
        }

        protected class Submission{
            protected static final String ID = "submission_id";
            protected static final String USER_NAME = "user_name";
            protected static final String CONTENT = "content";
            protected static final String INFORMATION_PARSED = "information_parsed";
            protected static final String CONTENT_TYPE = "content_type";
        }
    }

    private DatabaseClient client;
    private JSONDocumentManager docMgr;
    DocumentUriTemplate template;




    public DatabaseHelper(){
        client = DatabaseClientFactory.newClient("localhost", 7998, "root", "root", DatabaseClientFactory.Authentication.DIGEST);
        docMgr = client.newJSONDocumentManager();
        template = docMgr.newDocumentUriTemplate("json");
    }

    public void release(){
        client.release();
    }

    public void query(){

        QueryManager qMgr = client.newQueryManager();

        JSONDocumentManager jdm = client.newJSONDocumentManager();

        StructuredQueryBuilder qb = qMgr.newStructuredQueryBuilder();
        StructuredQueryDefinition criteria = qb.collection(DbCollections.USER_COLLECTION);
        DocumentPage documents = jdm.search(criteria, 1);
        System.out.println("Total matching documents: " + documents.getTotalSize());
        for (DocumentRecord document: documents) {
            System.out.println(document.getUri());
            JacksonHandle content = document.getContent(new JacksonHandle());
            JsonNode node = content.get();
            System.out.println(node.toString());
            // Do something with the content using document.getContent()
        }

//        SearchHandle results = qMgr.search(criteria, new SearchHandle());

//        System.out.println(results.getTotalResults());

    }

    public boolean saveComment(String id, String userName, String content){
        return saveSubmission(id, userName, content, COMMENT);
    }

    public boolean savePost(String id, String userName, String content){
        return saveSubmission(id, userName, content, SUBMISSION);
    }

    private boolean saveSubmission(String id, String userName, String content, String contentType){

        DocumentRecord submission = findSubmission(id);
        if (submission != null){
            return true;
        }
        JsonNodeFactory nodeFactory = JsonNodeFactory.instance;

        ObjectNode node = nodeFactory.objectNode();
        node.put(JsonProperties.Submission.ID, id);
        node.put(JsonProperties.Submission.USER_NAME, userName);
        node.put(JsonProperties.Submission.CONTENT, content);
        node.put(JsonProperties.Submission.CONTENT_TYPE, contentType);
        node.put(JsonProperties.Submission.INFORMATION_PARSED, false);

        DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
        metadataHandle.getCollections().addAll(DbCollections.SUBMISSION_COLLECTION);

        JSONWriteHandle writeHandle = new JacksonHandle(node);

        docMgr.create(template, metadataHandle, writeHandle);

        return true;
    }


    private DocumentRecord findSubmission(String id){
        QueryManager qMgr = client.newQueryManager();
        StructuredQueryBuilder sb = qMgr.newStructuredQueryBuilder();

        StructuredQueryDefinition criteria = sb.and(
                sb.collection(DbCollections.SUBMISSION_COLLECTION),
                sb.containerQuery(sb.jsonProperty(JsonProperties.Submission.ID), sb.term(id))
        );

        DocumentPage documents = docMgr.search(criteria, 1);
        if (documents.getTotalSize() != 0){
            return documents.next();
        }
        return null;

    }

    private DocumentRecord findSubmission(String userName, String content){
        QueryManager qMgr = client.newQueryManager();
        StructuredQueryBuilder sb = qMgr.newStructuredQueryBuilder();

        StructuredQueryDefinition criteria = sb.and(
                sb.collection(DbCollections.SUBMISSION_COLLECTION),
                sb.containerQuery(sb.jsonProperty(JsonProperties.Submission.USER_NAME), sb.term(userName)),
                sb.containerQuery(sb.jsonProperty(JsonProperties.Submission.CONTENT), sb.term(content))
        );

        DocumentPage documents = docMgr.search(criteria, 1);
        if (documents.getTotalSize() != 0){
            return documents.next();
        }
        return null;

    }

    private DocumentRecord findUser(String userName){
        QueryManager qMgr = client.newQueryManager();
        StructuredQueryBuilder sb = qMgr.newStructuredQueryBuilder();

        StructuredQueryDefinition criteria = sb.and(sb.collection(DbCollections.USER_COLLECTION),
                                                       sb.containerQuery(sb.jsonProperty(JsonProperties.User.USER_NAME), sb.term(userName))
                                                      );

        DocumentPage documents = docMgr.search(criteria, 1);
        if (documents.getTotalSize() != 0){
            return documents.next();
        }
        return null;
    }

    public boolean modifyStatusForUser(String userName){
        DocumentRecord user = findUser(userName);
        if (user != null){
            JacksonHandle content = user.getContent(new JacksonHandle());
            JsonNode node = content.get();

            ObjectNode objNode = (ObjectNode)node;
            objNode.put(JsonProperties.User.INFORMATION_PARSED, false);
            content = new JacksonHandle(objNode);
            docMgr.write(user.getUri(), content);
            return true;
        }
        return false;
    }

    public boolean saveOrModifyStatusForUser(String userName){

        //user already exists, adding new submissions
        if (modifyStatusForUser(userName)){
            return  false;
        }

        JsonNodeFactory nodeFactory = JsonNodeFactory.instance;

        ObjectNode node = nodeFactory.objectNode();
        node.put(JsonProperties.User.USER_NAME, userName);
        node.put(JsonProperties.User.INFORMATION_PARSED, false);

        DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
        metadataHandle.getCollections().addAll(DbCollections.USER_COLLECTION);

        JSONWriteHandle writeHandle = new JacksonHandle(node);

        docMgr.create(template, metadataHandle, writeHandle);

        return true;
    }





}
