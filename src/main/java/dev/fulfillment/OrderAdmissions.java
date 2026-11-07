package dev.fulfillment;

import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

record OrderKey(String tenant,String requestKey) {}
record Admission(OrderKey key,OrderRequest request) {}
record PendingOrder(Admission admission,CompletableFuture<OrderView> result) {}

/** Amortizes durable commits across concurrent HTTP calls. A 202 waits for commit. */
@Service
@Profile("api")
class OrderAdmissions {
    private final BlockingQueue<PendingOrder> queue=new ArrayBlockingQueue<>(4096);
    private final AtomicBoolean closing=new AtomicBoolean();
    private final Thread writer;
    private final Ledger ledger;

    OrderAdmissions(Ledger ledger) {
        this.ledger=ledger;
        writer=Thread.ofPlatform().name("order-admission-writer").daemon(true).start(this::write);
    }

    OrderView accept(String tenant,String key,OrderRequest request) {
        var pending=new PendingOrder(new Admission(new OrderKey(tenant,key),request),new CompletableFuture<>());
        if (closing.get() || !queue.offer(pending)) throw unavailable();
        // Cover a shutdown racing with the enqueue.
        if (closing.get() && queue.remove(pending)) throw unavailable();
        try { return pending.result().join(); }
        catch (CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException cause) throw cause;
            throw failure;
        }
    }

    private void write() {
        while (!closing.get()) {
            List<PendingOrder> batch=new ArrayList<>(128);
            try {
                batch.add(queue.take());
                PendingOrder next=queue.poll(1,TimeUnit.MILLISECONDS);
                if (next!=null) batch.add(next);
                queue.drainTo(batch,128-batch.size());
                var saved=ledger.acceptBatch(batch.stream().map(PendingOrder::admission).toList());
                for (var pending:batch) {
                    var request=pending.admission().request();
                    OrderView view=saved.get(pending.admission().key());
                    if (!view.warehouse().equals(request.warehouse()) || !view.sku().equals(request.sku())
                            || view.quantity()!=request.quantity()) {
                        pending.result().completeExceptionally(new ResponseStatusException(HttpStatus.CONFLICT,
                            "Key reused with different order"));
                    } else pending.result().complete(view);
                }
            } catch (InterruptedException stopping) {
                batch.forEach(item -> item.result().completeExceptionally(unavailable()));
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException failure) {
                batch.forEach(item -> item.result().completeExceptionally(failure));
            }
        }
    }

    private ResponseStatusException unavailable() {
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"Admission queue unavailable; retry with the same key");
    }

    @PreDestroy void stop() {
        closing.set(true);
        writer.interrupt();
        PendingOrder item;
        while ((item=queue.poll())!=null) item.result().completeExceptionally(unavailable());
    }
}
