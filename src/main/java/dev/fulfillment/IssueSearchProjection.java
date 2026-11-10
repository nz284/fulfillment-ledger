package dev.fulfillment;

import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Profile("search")
@Transactional
class IssueSearchProjection {
    private final IssueSearchStore store;
    private final IssueSearchIndex index;
    private final int pageSize;
    IssueSearchProjection(IssueSearchStore store,IssueSearchIndex index,@Value("${app.search.page-size:200}") int pageSize) {
        if(pageSize<1 || pageSize>500) throw new IllegalArgumentException("Search page size must be 1..500");
        this.store=store; this.index=index; this.pageSize=pageSize;
    }

    // This lock is private to search. Order, reconciliation and review writers never acquire it.
    void changes(List<IssueChange> batch) {
        var state=store.generation(true);
        if(state.active()==null && state.building()==null) throw new IllegalStateException("Index is initializing");
        var ids=new HashSet<UUID>();
        for(var change:batch) ids.add(Objects.requireNonNull(change.issueId()));
        var docs=store.documents(ids);
        if(docs.size()!=ids.size()) throw new IllegalStateException("Missing issue snapshot");
        if(state.building()!=null) index.write(state.building(),docs);
        if(state.active()!=null && (state.building()==null || index.exists(state.active())))
            index.write(state.active(),docs);
    }

    void advanceBuild() {
        var state=store.generation(true);
        if(state.retired()!=null) {
            index.remove(state.retired());
            store.retiredRemoved();
        }
        if(state.building()==null) {
            if(state.active()==null || !index.exists(state.active())) store.requestBuild();
            return;
        }
        String target=state.building();
        UUID cursor=state.cursor();
        if(!index.exists(target)) {
            if(cursor!=null) {
                // Commit the rewind before recreating the external index. A crash in between is safe.
                store.progress(null);
                return;
            }
            index.create(target);
        }
        var ids=store.page(cursor,pageSize);
        index.write(target,store.documents(ids));
        if(ids.size()==pageSize) {
            store.progress(ids.getLast());
        } else {
            index.switchAlias(target);
            store.activate();
        }
    }
}
