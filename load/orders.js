import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const rate = Number(__ENV.RATE || 5);
const base = __ENV.BASE_URL || 'http://localhost:8080';
const run = __ENV.RUN_ID;
const warmSeconds = Number(__ENV.WARM_SECONDS || 0);
if (!run) throw new Error('Set RUN_ID to a unique test identifier');
export const options = {
  summaryTrendStats: ['avg','med','p(95)','p(99)','max'],
  scenarios: { orders: { executor: 'constant-arrival-rate', rate, timeUnit: '1s',
    duration: __ENV.DURATION || '30s', preAllocatedVUs: Number(__ENV.VUS || 20),
    maxVUs: Number(__ENV.MAX_VUS || 100) } },
  thresholds: { http_req_failed: ['rate<0.01'], dropped_iterations: ['count==0'], checks: ['rate>0.99'],
    'http_req_duration{name:create}': ['p(99)<60000'],
    'http_req_duration{name:create,phase:measurement}': ['p(99)<60000'],
    'http_req_failed{phase:measurement}': ['rate==0'] }
};
export default function () {
  const iteration = exec.scenario.iterationInTest;
  // Keep connections, virtual users and the arrival schedule continuous across the boundary.
  const warming = Date.now() - exec.scenario.startTime < warmSeconds * 1000;
  const phase = warming ? 'warmup' : 'measurement';
  const headers = {'Content-Type':'application/json','X-Tenant':warming ? run+'-lead' : run,'Idempotency-Key':String(iteration)};
  const warehouse = iteration % 2 ? 'A' : 'B';
  const order = http.post(base+'/orders', JSON.stringify({ warehouse, sku:'demo',quantity:1 }), {headers, tags:{name:'create',phase}});
  check(order, {'accepted': r => r.status === 202});
  http.get(base+'/warehouses/'+warehouse,{tags:{name:'directory',phase}});
  if (order.status === 202) http.get(base+'/orders/'+order.json('id'),{headers,tags:{name:'read order',phase}});
}
