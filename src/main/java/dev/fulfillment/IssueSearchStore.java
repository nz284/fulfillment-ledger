package dev.fulfillment;

import jakarta.persistence.EntityManager;
import java.util.*;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

record IssueChange(UUID issueId,long version) {}
record SearchDocument(UUID id,long version,JsonNode fields) {}
record Generation(String active,String building,UUID cursor,String retired) {}

@Service
@Profile("search")
@Transactional
class IssueSearchStore {
    private final EntityManager em;
    private final ObjectMapper json;
    IssueSearchStore(EntityManager em,ObjectMapper json) { this.em=em; this.json=json; }

    List<IssueChange> pending() {
        List<Object[]> rows=em.createNativeQuery("""
            SELECT issue_id,version FROM issue_search_outbox ORDER BY queued_at,issue_id LIMIT 200
            """,Object[].class).getResultList();
        return rows.stream().map(r -> new IssueChange((UUID)r[0],(Long)r[1])).toList();
    }

    void acknowledged(List<IssueChange> batch) {
        em.createNativeQuery("""
            DELETE FROM issue_search_outbox o USING jsonb_to_recordset(cast(:batch AS jsonb))
            AS b("issueId" uuid,version bigint) WHERE o.issue_id=b."issueId" AND o.version=b.version
            """).setParameter("batch",json.writeValueAsString(batch)).executeUpdate();
    }

    Generation generation(boolean lock) {
        Object[] r=(Object[])em.createNativeQuery("SELECT active_index,building_index,cursor_id,retired_index "+
            "FROM issue_search_generation WHERE singleton"+(lock ? " FOR UPDATE" : "")).getSingleResult();
        return new Generation((String)r[0],(String)r[1],(UUID)r[2],(String)r[3]);
    }

    Generation requestBuild() {
        var current=generation(true);
        if (current.building()==null) {
            em.createNativeQuery("UPDATE issue_search_generation SET building_index=:name,cursor_id=NULL")
                .setParameter("name","ledger-issues-"+UUID.randomUUID()).executeUpdate();
        }
        return generation(false);
    }

    void progress(UUID id) {
        em.createNativeQuery("UPDATE issue_search_generation SET cursor_id=cast(:id AS uuid)")
            .setParameter("id",id).executeUpdate();
    }
    void activate() {
        em.createNativeQuery("""
            UPDATE issue_search_generation SET retired_index=active_index,active_index=building_index,
            building_index=NULL,cursor_id=NULL
            """).executeUpdate();
    }
    void retiredRemoved() { em.createNativeQuery("UPDATE issue_search_generation SET retired_index=NULL").executeUpdate(); }

    List<UUID> page(UUID after,int limit) {
        var q=em.createNativeQuery("SELECT id FROM reconciliation_issue "+
            (after==null ? "" : "WHERE id>:after ")+"ORDER BY id",UUID.class).setMaxResults(limit);
        if (after!=null) q.setParameter("after",after);
        return q.getResultList();
    }

    List<SearchDocument> documents(Collection<UUID> ids) {
        if (ids.isEmpty()) return List.of();
        // A single MVCC snapshot supplies both the version and all review notes.
        List<Object[]> rows=em.createNativeQuery("""
            SELECT i.id,i.search_version,cast(jsonb_build_object(
                'issueId',i.id,'orderId',i.order_id,'kind',i.kind,'status',i.status,
                'firstSeen',i.first_seen,'lastSeen',i.last_seen,
                'warehouse',ARRAY(SELECT DISTINCT p->>'warehouse' FROM jsonb_each(cast(i.latest_evidence AS jsonb)) e(k,p)
                                  WHERE p->>'warehouse' IS NOT NULL),
                'sku',ARRAY(SELECT DISTINCT p->>'sku' FROM jsonb_each(cast(i.latest_evidence AS jsonb)) e(k,p)
                            WHERE p->>'sku' IS NOT NULL),
                'notes',COALESCE((SELECT jsonb_agg(r.note ORDER BY r.id) FROM reconciliation_review r WHERE r.issue_id=i.id),
                                 cast('[]' AS jsonb))) AS text)
            FROM reconciliation_issue i WHERE i.id IN (:ids)
            """,Object[].class).setParameter("ids",ids).getResultList();
        return rows.stream().map(r->new SearchDocument((UUID)r[0],(Long)r[1],json.readTree((String)r[2]))).toList();
    }
}
