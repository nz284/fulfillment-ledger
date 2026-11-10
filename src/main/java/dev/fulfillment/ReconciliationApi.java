package dev.fulfillment;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@Profile("reconciler")
@RequestMapping("/reconciliation")
class ReconciliationApi {
    private final ReconciliationStore store;
    ReconciliationApi(ReconciliationStore store) { this.store=store; }

    @PostMapping("/runs") @ResponseStatus(HttpStatus.ACCEPTED)
    RunView request(@RequestParam Instant from,@RequestParam Instant to) { return store.request(from,to); }

    @GetMapping("/runs/{id}") RunView get(@PathVariable UUID id) { return store.get(id); }
    @GetMapping("/runs") List<RunView> runs(@RequestParam(defaultValue="50") int limit) { return store.runs(limit); }

    @PostMapping("/runs/{id}/rerun") @ResponseStatus(HttpStatus.ACCEPTED)
    RunView rerun(@PathVariable UUID id) { return store.rerun(id); }

    @GetMapping("/issues")
    List<ReconciliationStore.Issue> issues(@RequestParam(required=false) UUID orderId,
            @RequestParam(required=false) UUID after,@RequestParam(defaultValue="100") int limit) {
        return store.issues(orderId,after,limit);
    }

    record Review(String actor,String note) {}
    @GetMapping("/issues/{id}/reviews")
    List<ReconciliationStore.ReviewEntry> reviews(@PathVariable UUID id) { return store.reviews(id); }
    @PostMapping("/issues/{id}/review") @ResponseStatus(HttpStatus.NO_CONTENT)
    void review(@PathVariable UUID id,@RequestBody Review review) { store.review(id,review.actor(),review.note()); }
}
