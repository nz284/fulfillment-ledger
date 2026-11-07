import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const rate = Number(__ENV.RATE || 5);
const base = __ENV.BASE_URL || 'http://localhost:8080';
const run = __ENV.RUN_ID;
if (!run) throw new Error('Set RUN_ID to a unique test identifier');
export const options = {
  scenarios: { orders: { executor: 'constant-arrival-rate', rate, timeUnit: '1s',
    duration: __ENV.DURATION || '30s', preAllocatedVUs: Number(__ENV.VUS || 20),
    maxVUs: Number(__ENV.MAX_VUS || 100) } },
  thresholds: { http_req_failed: ['rate<0.01'], dropped_iterations: ['count==0'], checks: ['rate>0.99'] }
};
export default function () {
  const iteration = exec.scenario.iterationInTest;
  const headers = {'Content-Type':'application/json','X-Tenant':run,'Idempotency-Key':String(iteration)};
  const warehouse = iteration % 2 ? 'A' : 'B';
  const order = http.post(base+'/orders', JSON.stringify({ warehouse, sku:'demo',quantity:1 }), {headers, tags:{name:'create'}});
  check(order, {'accepted': r => r.status === 202});
  http.get(base+'/warehouses/'+warehouse,{tags:{name:'directory'}});
  if (order.status === 202) http.get(base+'/orders/'+order.json('id'),{headers,tags:{name:'read order'}});
}
