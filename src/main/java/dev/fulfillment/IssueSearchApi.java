package dev.fulfillment;

import java.time.Instant;
import java.util.*;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClientException;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.JsonNode;

@RestController
@Profile("search")
@RequestMapping("/issue-search")
class IssueSearchApi {
    private final IssueSearchIndex index;
    private final IssueSearchStore store;
    IssueSearchApi(IssueSearchIndex index,IssueSearchStore store) { this.index=index; this.store=store; }

    record Results(long total,List<JsonNode> issues,JsonNode aggregations) {}
    @GetMapping
    Results search(@RequestParam(defaultValue="") String q,@RequestParam(required=false) String warehouse,
            @RequestParam(required=false) String sku,@RequestParam(required=false) String kind,
            @RequestParam(required=false) String status,@RequestParam(required=false) Instant from,
            @RequestParam(required=false) Instant to,@RequestParam(defaultValue="0") int offset,
            @RequestParam(defaultValue="25") int limit,@RequestParam(defaultValue="10") int buckets) {
        if(q.length()>500 || limit<1 || limit>100 || offset<0 || offset>10000-limit || buckets<1 || buckets>100 ||
                (from!=null && to!=null && !from.isBefore(to)))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Invalid text, date range or page limits");
        List<Object> filters=new ArrayList<>();
        addTerm(filters,"warehouse",warehouse); addTerm(filters,"sku",sku);
        addTerm(filters,"kind",kind); addTerm(filters,"status",status);
        Map<String,String> dates=new HashMap<>();
        if(from!=null) dates.put("gte",from.toString());
        if(to!=null) dates.put("lt",to.toString());
        if(!dates.isEmpty()) filters.add(Map.of("range",Map.of("firstSeen",dates)));
        Map<String,Object> bool=new HashMap<>();
        bool.put("filter",filters);
        if(!q.isBlank()) bool.put("must",Map.of("match",Map.of("notes",Map.of("query",q,"operator","and"))));
        Map<String,Object> aggs=new HashMap<>();
        for(String field:List.of("warehouse","sku","kind","status"))
            aggs.put(field,Map.of("terms",Map.of("field",field,"size",buckets)));
        try {
            var result=index.search(Map.of("query",Map.of("bool",bool),"aggs",aggs,"from",offset,"size",limit,
                "track_total_hits",true,"sort",List.of(Map.of("firstSeen","desc"),Map.of("issueId","asc"))));
            var hits=result.path("hits");
            List<JsonNode> issues=new ArrayList<>();
            for(var hit:hits.path("hits")) issues.add(hit.path("_source"));
            return new Results(hits.path("total").path("value").asLong(),issues,result.path("aggregations"));
        } catch(RestClientException | IllegalStateException unavailable) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"Search index unavailable",unavailable);
        }
    }

    private void addTerm(List<Object> filters,String field,String value) {
        if(value!=null) filters.add(Map.of("term",Map.of(field,value)));
    }
    @PostMapping("/rebuild") @ResponseStatus(HttpStatus.ACCEPTED)
    Generation rebuild() { return store.requestBuild(); }
    @GetMapping("/generation") Generation generation() { return store.generation(false); }
}
