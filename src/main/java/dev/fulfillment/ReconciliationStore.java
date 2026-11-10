package dev.fulfillment;

import jakarta.persistence.*;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.ObjectMapper;

@Service
@Transactional
class ReconciliationStore {
    private final EntityManager em;
    private final Ledger ledger;
    private final ObjectMapper json;
    private final Duration grace;

    ReconciliationStore(EntityManager em,Ledger ledger,ObjectMapper json,
            @Value("${app.reconciliation.grace:5m}") Duration grace) {
        if (grace.isNegative()) throw new IllegalArgumentException("Negative reconciliation grace");
        this.em=em; this.ledger=ledger; this.json=json; this.grace=grace;
    }

    private Instant dbNow() {
        return ScanKey.instant(em.createNativeQuery("SELECT clock_timestamp()").getSingleResult());
    }

    RunView request(Instant from,Instant to) {
        from=from.truncatedTo(ChronoUnit.MICROS);
        to=to.truncatedTo(ChronoUnit.MICROS);
        if (!from.isBefore(to) || Duration.between(from,to).compareTo(Duration.ofDays(1))>0 ||
                to.isAfter(dbNow().minus(grace)))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Use a closed window of at most 24h ending before the grace cutoff");
        return create(from,to);
    }

    private RunView create(Instant from,Instant to) {
        em.createNativeQuery("""
            INSERT INTO reconciliation_run(id,starts_at,ends_at) VALUES (gen_random_uuid(),:lo,:hi)
            ON CONFLICT(starts_at,ends_at) DO NOTHING
            """).setParameter("lo",from).setParameter("hi",to).executeUpdate();
        return em.createQuery("from ReconciliationRun where startsAt=:lo and endsAt=:hi",ReconciliationRun.class)
            .setParameter("lo",from).setParameter("hi",to).getSingleResult().view();
    }

    void scheduleClosedWindow() {
        var rows=em.createNativeQuery("SELECT next_start FROM reconciliation_schedule FOR UPDATE SKIP LOCKED").getResultList();
        if (rows.isEmpty()) return;
        Instant from=ScanKey.instant(rows.getFirst()), to=from.plus(1,ChronoUnit.HOURS);
        if (to.isAfter(dbNow().minus(grace))) return;
        create(from,to);
        em.createNativeQuery("UPDATE reconciliation_schedule SET next_start=:next").setParameter("next",to).executeUpdate();
    }

    RunView get(UUID id) { return load(id,false).view(); }

    List<RunView> runs(int limit) {
        if (limit<1 || limit>500) throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        return em.createQuery("from ReconciliationRun order by endsAt desc,id",ReconciliationRun.class)
            .setMaxResults(limit).getResultList().stream().map(ReconciliationRun::view).toList();
    }

    private ReconciliationRun load(UUID id,boolean lock) {
        var run=em.find(ReconciliationRun.class,id,lock ? LockModeType.PESSIMISTIC_WRITE : LockModeType.NONE);
        if (run==null) throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        return run;
    }

    RunView rerun(UUID id) {
        var run=load(id,true);
        if (run.phase.equals("DONE")) {
            run.phase="ORDERS"; run.cursorAt=null; run.cursorId=null;
            run.revision++; run.pages=0; run.failures=0; run.lastError=null; run.readyAt=dbNow();
        }
        return run.view();
    }

    Optional<RunView> next() {
        List<ReconciliationRun> rows=em.createNativeQuery("""
            SELECT * FROM reconciliation_run WHERE phase<>'DONE' AND ready_at<=now() ORDER BY ready_at,id LIMIT 1
            """,ReconciliationRun.class).getResultList();
        return rows.stream().findFirst().map(ReconciliationRun::view);
    }

    List<ScanKey> orderKeys(RunView run,int limit) {
        String cursor=run.cursorId()==null ? "" : " AND (created_at,id)>(:at,:id)";
        var q=em.createNativeQuery("SELECT created_at,id FROM fulfillment_order WHERE created_at>=:lo AND created_at<:hi"+
            cursor+" ORDER BY created_at,id",Object[].class).setParameter("lo",run.startsAt()).setParameter("hi",run.endsAt())
            .setMaxResults(limit);
        if (run.cursorId()!=null) q.setParameter("at",run.cursorAt()).setParameter("id",run.cursorId());
        List<Object[]> rows=q.getResultList();
        return rows.stream().map(row -> new ScanKey(ScanKey.instant(row[0]),(UUID)row[1])).toList();
    }

    void failed(RunView snapshot,RuntimeException failure) {
        var run=load(snapshot.id(),true);
        if (run.revision!=snapshot.revision()) return;
        run.failures++; run.revision++;
        run.lastError=failure.getClass().getSimpleName();
        run.readyAt=dbNow().plusSeconds(Math.min(60,1L<<Math.min(6,run.failures)));
    }

    void apply(RunView snapshot,List<ScanKey> keys,Map<UUID,Shipment> remote,boolean lastPage) {
        var run=load(snapshot.id(),true);
        if (run.revision!=snapshot.revision()) return; // Another replica committed this page first.
        if (!keys.isEmpty()) {
            List<UUID> ids=keys.stream().map(ScanKey::id).toList();
            Map<UUID,OrderEntity> local=new HashMap<>();
            for (OrderEntity order:em.createQuery("from OrderEntity where id in :ids",OrderEntity.class)
                    .setParameter("ids",ids).getResultList()) local.put(order.id,order);
            List<UUID> matched=new ArrayList<>();
            Instant matureBefore=dbNow().minus(grace);
            for (UUID id:ids) {
                OrderEntity order=local.get(id);
                Shipment shipment=remote.get(id);
                if ((order==null && shipment==null) || (order!=null && order.createdAt.isAfter(matureBefore))) continue;
                String kind=classify(order,shipment);
                if (kind==null) matched.add(id);
                else if (kind.equals("UNCONFIRMED_MATCH")) {
                    repair(run.id,order,shipment);
                } else {
                    observe(run.id,id,kind,"OPEN",new Evidence(order==null ? null : order.view(),shipment));
                }
            }
            if (!matched.isEmpty()) em.createNativeQuery("""
                UPDATE reconciliation_issue SET status='CLEARED',last_seen=now(),run_id=:run
                WHERE order_id IN (:ids) AND status IN ('OPEN','REVIEWED')
                """).setParameter("run",run.id).setParameter("ids",matched).executeUpdate();
            var last=keys.getLast();
            run.cursorAt=last.at(); run.cursorId=last.id();
        }
        if (lastPage) {
            run.phase=run.phase.equals("ORDERS") ? "MANIFEST" : "DONE";
            run.cursorAt=null; run.cursorId=null;
        }
        run.pages++; run.revision++; run.failures=0; run.lastError=null; run.readyAt=dbNow();
    }

    private String classify(OrderEntity order,Shipment warehouse) {
        if (order==null) return "PLATFORM_MISSING";
        if (warehouse==null) return "WAREHOUSE_MISSING";
        if (!warehouse.equals(new Shipment(order.id,order.warehouse,order.sku,order.quantity))) return "PAYLOAD_MISMATCH";
        return switch(order.state) {
            case "CONFIRMED" -> null;
            case "UNCONFIRMED" -> "UNCONFIRMED_MATCH";
            default -> "STATE_PENDING";
        };
    }

    private void repair(UUID run,OrderEntity order,Shipment shipment) {
        // Use the same lock order as ShippingWorker: task, then order. Recheck after locking.
        ledger.enqueue(order.id,order.id,"SHIP_"+order.warehouse,"submit");
        WorkItem task=em.find(WorkItem.class,order.id,LockModeType.PESSIMISTIC_WRITE);
        em.refresh(order,LockModeType.PESSIMISTIC_WRITE);
        String kind=classify(order,shipment);
        if (kind==null) {
            if (!task.done) { task.done=true; task.token++; }
            return;
        }
        if (!kind.equals("UNCONFIRMED_MATCH")) {
            observe(run,order.id,kind,"OPEN",new Evidence(order.view(),shipment));
            return;
        }
        observe(run,order.id,kind,"AUTO_REPAIRED",new Evidence(order.view(),shipment));
        task.done=true; task.token++;
        order.state="CONFIRMED";
        ledger.recordState(order);
    }

    private void observe(UUID run,UUID order,String kind,String status,Evidence evidence) {
        em.createNativeQuery("""
            UPDATE reconciliation_issue SET status='CLEARED',last_seen=now(),run_id=:run
            WHERE order_id=:ord AND kind<>:kind AND status IN ('OPEN','REVIEWED')
            """).setParameter("ord",order).setParameter("kind",kind).setParameter("run",run).executeUpdate();
        em.createNativeQuery("""
            INSERT INTO reconciliation_issue(id,order_id,kind,status,first_evidence,latest_evidence,run_id)
            VALUES (gen_random_uuid(),:ord,:kind,:status,:evidence,:evidence,:run)
            ON CONFLICT(order_id,kind) DO UPDATE SET latest_evidence=excluded.latest_evidence,
              status=CASE WHEN reconciliation_issue.status='REVIEWED'
                AND reconciliation_issue.latest_evidence=excluded.latest_evidence THEN 'REVIEWED' ELSE excluded.status END,
              last_seen=now(),run_id=excluded.run_id
            """).setParameter("ord",order).setParameter("kind",kind).setParameter("status",status)
            .setParameter("evidence",json.writeValueAsString(evidence)).setParameter("run",run).executeUpdate();
    }

    record Issue(UUID id,UUID orderId,String kind,String status,Evidence firstEvidence,Evidence latestEvidence,UUID lastRun) {}
    List<Issue> issues(UUID order,UUID after,int limit) {
        return findIssues(order,after,null,limit);
    }

    Issue issue(UUID id) {
        return findIssues(null,null,id,1).stream().findFirst().orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    private List<Issue> findIssues(UUID order,UUID after,UUID id,int limit) {
        if (limit<1 || limit>500) throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        String filter=(order==null ? "" : " AND order_id=:ord") + (after==null ? "" : " AND id>:after") +
            (id==null ? "" : " AND id=:id");
        var q=em.createNativeQuery("SELECT id,order_id,kind,status,first_evidence,latest_evidence,run_id "+
            "FROM reconciliation_issue WHERE true"+filter+" ORDER BY id",Object[].class).setMaxResults(limit);
        if (order!=null) q.setParameter("ord",order);
        if (after!=null) q.setParameter("after",after);
        if (id!=null) q.setParameter("id",id);
        List<Object[]> rows=q.getResultList();
        return rows.stream().map(r->new Issue((UUID)r[0],(UUID)r[1],(String)r[2],(String)r[3],
            json.readValue((String)r[4],Evidence.class),json.readValue((String)r[5],Evidence.class),(UUID)r[6])).toList();
    }

    record ReviewEntry(String actor,String note,Instant recordedAt) {}
    List<ReviewEntry> reviews(UUID id) {
        if (em.createNativeQuery("SELECT id FROM reconciliation_issue WHERE id=:id").setParameter("id",id).getResultList().isEmpty())
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        List<Object[]> rows=em.createNativeQuery("""
            SELECT actor,note,recorded_at FROM reconciliation_review WHERE issue_id=:id ORDER BY id
            """,Object[].class).setParameter("id",id).getResultList();
        return rows.stream().map(r->new ReviewEntry((String)r[0],(String)r[1],ScanKey.instant(r[2]))).toList();
    }

    void review(UUID id,String actor,String note) {
        if (actor==null || actor.isBlank() || actor.length()>100 || note==null || note.isBlank() || note.length()>2000)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Provide an actor and a review note");
        var statuses=em.createNativeQuery("SELECT status FROM reconciliation_issue WHERE id=:id FOR UPDATE")
            .setParameter("id",id).getResultList();
        if (statuses.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        if (!statuses.getFirst().equals("OPEN")) throw new ResponseStatusException(HttpStatus.CONFLICT,"Issue is no longer open");
        em.createNativeQuery("UPDATE reconciliation_issue SET status='REVIEWED' WHERE id=:id").setParameter("id",id).executeUpdate();
        em.createNativeQuery("INSERT INTO reconciliation_review(issue_id,actor,note) VALUES (:id,:actor,:note)")
            .setParameter("id",id).setParameter("actor",actor).setParameter("note",note).executeUpdate();
    }
}
