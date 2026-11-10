package dev.fulfillment;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
@Profile("search")
class IssueSearchIndex {
    static final String ALIAS="ledger-issues";
    private final RestClient es;
    private final ObjectMapper json;

    IssueSearchIndex(RestClient.Builder builder,ObjectMapper json,@Value("${app.search.url:http://elasticsearch:9200}") String url) {
        var factory=new JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build());
        factory.setReadTimeout(Duration.ofSeconds(8));
        es=builder.baseUrl(url).requestFactory(factory).build();
        this.json=json;
    }

    boolean exists(String index) {
        try { es.head().uri("/{index}",index).retrieve().toBodilessEntity(); return true; }
        catch(HttpClientErrorException.NotFound missing) { return false; }
    }

    void create(String index) {
        Map<String,Object> fields=new HashMap<>();
        for(String f:List.of("issueId","orderId","warehouse","sku","kind","status"))
            fields.put(f,Map.of("type","keyword"));
        fields.put("notes",Map.of("type","text"));
        fields.put("firstSeen",Map.of("type","date"));
        fields.put("lastSeen",Map.of("type","date"));
        es.put().uri("/{index}",index).body(Map.of("settings",Map.of("number_of_shards",1,"number_of_replicas",0),
            "mappings",Map.of("dynamic","strict","properties",fields))).retrieve().toBodilessEntity();
    }

    void write(String index,List<SearchDocument> docs) {
        if(docs.isEmpty()) return;
        StringBuilder body=new StringBuilder();
        for(var doc:docs) {
            body.append(json.writeValueAsString(Map.of("index",Map.of("_id",doc.id(),"version",doc.version(),
                "version_type","external")))).append('\n');
            body.append(doc.fields()).append('\n');
        }
        JsonNode result=es.post().uri("/{index}/_bulk",index).contentType(MediaType.parseMediaType("application/x-ndjson"))
            .body(body.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)).retrieve().body(JsonNode.class);
        if(result==null || result.path("items").size()!=docs.size()) throw new IllegalStateException("Incomplete bulk result");
        for(JsonNode item:result.path("items")) {
            var op=item.path("index");
            var status=op.path("status").asInt();
            if(status==409 && op.path("error").path("type").asString().equals("version_conflict_engine_exception")) continue;
            if(status<200 || status>=300) throw new IllegalStateException("Bulk write failed: "+op.path("error"));
        }
    }

    void switchAlias(String target) {
        es.post().uri("/{index}/_refresh",target).retrieve().toBodilessEntity();
        List<Object> actions=new ArrayList<>();
        try {
            var aliases=es.get().uri("/_alias/{alias}",ALIAS).retrieve().body(JsonNode.class);
            for(String old:Objects.requireNonNull(aliases).propertyNames())
                if(!target.equals(old)) actions.add(Map.of("remove",Map.of("index",old,"alias",ALIAS)));
        } catch(HttpClientErrorException.NotFound firstBuild) { /* First generation or cluster recovery. */ }
        actions.add(Map.of("add",Map.of("index",target,"alias",ALIAS)));
        var response=es.post().uri("/_aliases").body(Map.of("actions",actions)).retrieve().body(JsonNode.class);
        if(response==null || !response.path("acknowledged").asBoolean() || response.path("errors").asBoolean(false))
            throw new IllegalStateException("Alias change was not acknowledged");
    }

    void remove(String index) {
        try { es.delete().uri("/{index}",index).retrieve().toBodilessEntity(); }
        catch(HttpClientErrorException.NotFound missing) { /* A previous attempt already removed it. */ }
    }

    JsonNode search(Map<String,Object> body) {
        var result=es.post().uri("/"+ALIAS+"/_search?allow_partial_search_results=false")
            .body(body).retrieve().body(JsonNode.class);
        if(result==null || result.path("timed_out").asBoolean()) throw new IllegalStateException("Search timed out");
        return result;
    }
}
