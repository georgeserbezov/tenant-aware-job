import { useEffect, useState } from 'react';

const TENANTS = ['tenant-1', 'tenant-2', 'tenant-3'];

const WAITING_BECAUSE = {
  GLOBAL_LIMIT: 'the whole server is at capacity',
  TENANT_LIMIT: 'this tenant is already running its maximum',
  TARGET_LIMIT: 'that target is busy',
};

function explain(job) {
  if (job.status === 'FAILED') return job.lastError;
  if (job.blockReason !== 'NONE') return WAITING_BECAUSE[job.blockReason];
  if (job.lastError) return `retrying after: ${job.lastError}`;
  return '';
}

export default function App() {
  const [tenant, setTenant] = useState(TENANTS[0]);
  const [jobs, setJobs] = useState([]);
  const [capacity, setCapacity] = useState(null);
  const [connected, setConnected] = useState(false);
  const [targetId, setTargetId] = useState('database');
  const [payload, setPayload] = useState('{"hello":"world"}');
  const [idempotencyKey, setIdempotencyKey] = useState('');
  const [notice, setNotice] = useState('');

  useEffect(() => {
    setJobs([]);
    setNotice('');
    const source = new EventSource(`/jobs/stream?tenantId=${encodeURIComponent(tenant)}`);

    source.addEventListener('snapshot', (e) => setJobs(JSON.parse(e.data)));
    source.addEventListener('capacity', (e) => setCapacity(JSON.parse(e.data)));
    source.addEventListener('job-update', (e) => {
      const job = JSON.parse(e.data);
      setJobs((current) =>
        [...current.filter((j) => j.id !== job.id), job].sort((a, b) =>
          a.createdAt.localeCompare(b.createdAt)
        )
      );
    });

    source.onopen = () => setConnected(true);
    source.onerror = () => setConnected(false);
    return () => source.close();
  }, [tenant]);

  async function enqueue(specs) {
    const results = await Promise.all(
      specs.map((spec) =>
        fetch('/jobs', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Tenant-Id': tenant },
          body: JSON.stringify({ tenantId: tenant, ...spec }),
        })
      )
    );
    return results;
  }

  async function submitForm(event) {
    event.preventDefault();
    setNotice('');
    const key = idempotencyKey.trim() || `ui-${Date.now()}`;
    const [response] = await enqueue([{ targetId, payload, idempotencyKey: key }]);

    if (response.status === 200) {
      setNotice(`Key "${key}" was already used - returned the existing job, nothing new enqueued.`);
    } else if (!response.ok) {
      const error = await response.json();
      setNotice(`Rejected: ${error.message || response.status}`);
    }
  }

  function demo(targets) {
    setNotice('');
    const stamp = Date.now();
    enqueue(
      targets.map((target, i) => ({
        targetId: target,
        payload: 'demo',
        idempotencyKey: `${stamp}-${i}`,
      }))
    );
  }

  const sixTargets = ['a', 'b', 'c', 'd', 'e', 'f'].map((n) => `target-${n}`);
  const waiting = jobs.filter((j) => j.blockReason !== 'NONE').length;

  return (
    <main>
      <h1>Tenant Job Scheduler</h1>

      <section className="bar">
        <label>
          Tenant{' '}
          <select value={tenant} onChange={(e) => setTenant(e.target.value)}>
            {TENANTS.map((t) => (
              <option key={t}>{t}</option>
            ))}
          </select>
        </label>
        <span className={connected ? 'dot live' : 'dot'} />
        <span className="muted">{connected ? 'live' : 'disconnected'}</span>
        {capacity && (
          <span className="muted">
            {capacity.globalAvailable}/{capacity.globalMax} server slots free
            {capacity.tenantAvailable != null &&
              ` · ${capacity.tenantAvailable}/${capacity.perTenantMax} for ${tenant}`}
          </span>
        )}
      </section>

      <form className="enqueue" onSubmit={submitForm}>
        <label>
          Target
          <input value={targetId} onChange={(e) => setTargetId(e.target.value)} required />
        </label>
        <label>
          Payload
          <input value={payload} onChange={(e) => setPayload(e.target.value)} required />
        </label>
        <label>
          Idempotency key <span className="muted">(blank = generated)</span>
          <input
            value={idempotencyKey}
            onChange={(e) => setIdempotencyKey(e.target.value)}
            placeholder="reuse one to test idempotency"
          />
        </label>
        <button type="submit">Enqueue job</button>
      </form>

      {notice && <p className="notice">{notice}</p>}

      <section className="bar">
        <span className="muted">Or trigger a cap:</span>
        <button onClick={() => demo(Array(6).fill('database'))}>
          Send 6 to the same target
        </button>
        <button onClick={() => demo(sixTargets)}>Send 6 to different targets</button>
      </section>

      {waiting > 0 && (
        <p className="banner">
          {waiting} job{waiting > 1 ? 's are' : ' is'} queued because a cap was hit.
        </p>
      )}

      <table>
        <thead>
          <tr>
            <th>Target</th>
            <th>Status</th>
            <th>Why</th>
            <th>Attempt</th>
          </tr>
        </thead>
        <tbody>
          {jobs.length === 0 && (
            <tr>
              <td colSpan="4" className="muted">
                No jobs yet for {tenant}. Send some above.
              </td>
            </tr>
          )}
          {jobs.map((job) => (
            <tr key={job.id}>
              <td>{job.targetId}</td>
              <td>
                <span className={`status ${job.status.toLowerCase()}`}>{job.status}</span>
              </td>
              <td className={job.status === 'FAILED' ? 'reason error' : 'reason'}>
                {explain(job)}
              </td>
              <td>
                {job.attempt}/{job.maxAttempts}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </main>
  );
}
