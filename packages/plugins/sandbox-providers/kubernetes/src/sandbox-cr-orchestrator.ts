/**
 * SandboxOrchestrator implementation backed by the kubernetes-sigs/agent-sandbox
 * Sandbox CRD (agents.x-k8s.io/v1beta1).
 *
 * The Sandbox CR creates a long-lived pod that paperclip-server can exec into
 * for multi-command adapter-install workflows — the key architectural win over
 * the batch/v1 Job backend.
 *
 * Key semantic differences from jobOrchestrator:
 * - claim() creates a Sandbox CR via CustomObjectsApi instead of a batch Job
 * - getStatus() maps v1beta1 conditions to SandboxStatus
 * - findPod() uses the v1beta1 status selector and verifies Sandbox ownership
 * - waitForCompletion() means "wait until pod is Ready to exec" NOT "wait until
 *   workload finishes". The Sandbox pod runs sleep infinity; execution completion
 *   is tracked by the individual execInPod() calls.
 * - release() deletes the Sandbox CR with Foreground propagation (controller
 *   tears down the underlying pod).
 *
 * NOTE: streamLogs() is provided for interface conformance but is limited —
 * the sleep-infinity pod has no meaningful stdout. Callers in execute mode
 * should use execInPod() and capture its stdout/stderr directly.
 */

import type { KubeClients } from "./kube-client.js";
import type { SandboxOrchestrator, SandboxStatus } from "./sandbox-orchestrator.js";
import {
  SANDBOX_API_VERSION,
  SANDBOX_GROUP,
  SANDBOX_PLURAL,
  SANDBOX_VERSION,
} from "./sandbox-cr-api.js";

export class SandboxCrTimeoutError extends Error {
  constructor(namespace: string, name: string, timeoutMs: number) {
    super(
      `Sandbox ${namespace}/${name} did not reach a current Ready condition within ${timeoutMs}ms`,
    );
    this.name = "SandboxCrTimeoutError";
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class SandboxIdentityMismatchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SandboxIdentityMismatchError";
  }
}

export class SandboxPodIdentityTimeoutError extends Error {
  constructor(namespace: string, name: string, timeoutMs: number) {
    super(
      `Sandbox ${namespace}/${name} did not produce an owned Pod with a UID within ${timeoutMs}ms`,
    );
    this.name = "SandboxPodIdentityTimeoutError";
  }
}

export interface SandboxPodIdentity {
  name: string;
  uid: string;
  phase?: string;
  terminating: boolean;
}

type SandboxCondition = {
  type?: string;
  status?: string;
  reason?: string;
  message?: string;
  observedGeneration?: number;
};

// ReconcilerError is intentionally excluded: v1.0.2 reports any reconciliation
// error through Ready and asks controller-runtime to retry, so it may recover.
const TERMINAL_READY_REASONS = new Set([
  "MultiplePods",
  "PodFailed",
  "PodSucceeded",
  "SandboxExpired",
  "SandboxSuspended",
]);

function getConditions(cr: Record<string, unknown>): SandboxCondition[] {
  const status = (cr.status as Record<string, unknown>) ?? {};
  return Array.isArray(status.conditions)
    ? (status.conditions as SandboxCondition[])
    : [];
}

function getCondition(
  conditions: SandboxCondition[],
  type: string,
): SandboxCondition | undefined {
  return conditions.find((condition) => condition.type === type);
}

function isConditionCurrent(
  condition: SandboxCondition | undefined,
  cr: Record<string, unknown>,
): boolean {
  if (!condition) return false;
  const metadata = (cr.metadata as Record<string, unknown>) ?? {};
  const generation = metadata.generation;
  if (typeof generation !== "number") return true;
  return (
    typeof condition.observedGeneration === "number" &&
    condition.observedGeneration >= generation
  );
}

function getTerminalStatus(
  conditions: SandboxCondition[],
  cr: Record<string, unknown>,
): SandboxStatus | undefined {
  const finished = getCondition(conditions, "Finished");
  if (finished?.status === "True" && isConditionCurrent(finished, cr)) {
    if (finished.reason === "PodSucceeded") {
      return {
        phase: "Succeeded",
        complete: true,
        active: 0,
        succeeded: 1,
        failed: 0,
        reason: finished.reason,
        message: finished.message,
      };
    }
    return {
      phase: "Failed",
      complete: true,
      active: 0,
      succeeded: 0,
      failed: 1,
      reason: finished.reason ?? "PodFailed",
      message: finished.message,
    };
  }

  const ready = getCondition(conditions, "Ready");
  if (
    ready?.status === "False" &&
    isConditionCurrent(ready, cr) &&
    ready.reason &&
    TERMINAL_READY_REASONS.has(ready.reason)
  ) {
    if (ready.reason === "PodSucceeded") {
      return {
        phase: "Succeeded",
        complete: true,
        active: 0,
        succeeded: 1,
        failed: 0,
        reason: ready.reason,
        message: ready.message,
      };
    }
    return {
      phase: "Failed",
      complete: true,
      active: 0,
      succeeded: 0,
      failed: 1,
      reason: ready.reason,
      message: ready.message,
    };
  }

  const failed = getCondition(conditions, "Failed");
  if (failed?.status === "True" && isConditionCurrent(failed, cr)) {
    return {
      phase: "Failed",
      complete: true,
      active: 0,
      succeeded: 0,
      failed: 1,
      reason: failed.reason,
      message: failed.message,
    };
  }
  return undefined;
}

/** Map v1beta1 conditions to the provider's Job-like status contract. */
function mapSandboxStatus(cr: Record<string, unknown>): SandboxStatus {
  const conditions = getConditions(cr);
  const terminal = getTerminalStatus(conditions, cr);
  if (terminal) return terminal;

  const metadata = (cr.metadata as Record<string, unknown>) ?? {};
  if (metadata.deletionTimestamp) {
    return {
      phase: "Running",
      complete: false,
      active: 0,
      succeeded: 0,
      failed: 0,
      reason: "Terminating",
    };
  }

  const ready = getCondition(conditions, "Ready");
  if (ready?.status === "True" && isConditionCurrent(ready, cr)) {
    return {
      phase: "Running",
      complete: false,
      active: 1,
      succeeded: 0,
      failed: 0,
    };
  }

  const suspended = getCondition(conditions, "Suspended");
  if (suspended?.status === "True" && isConditionCurrent(suspended, cr)) {
    return {
      phase: "Failed",
      complete: false,
      active: 0,
      succeeded: 0,
      failed: 1,
      reason: suspended.reason ?? "SandboxSuspended",
      message: suspended.message,
    };
  }

  return {
    phase: "Pending",
    complete: false,
    active: 0,
    succeeded: 0,
    failed: 0,
    reason: ready?.reason,
    message: ready?.message,
  };
}

export async function createSandboxCr(
  clients: KubeClients,
  namespace: string,
  manifest: Record<string, unknown>,
): Promise<{ uid: string }> {
  const result = await clients.custom.createNamespacedCustomObject({
    group: SANDBOX_GROUP,
    version: SANDBOX_VERSION,
    namespace,
    plural: SANDBOX_PLURAL,
    body: manifest,
  });
  const uid = (result as { metadata?: { uid?: string } }).metadata?.uid;
  if (!uid) throw new Error("Sandbox CR created without a UID");
  return { uid };
}

export async function getSandboxCrStatus(
  clients: KubeClients,
  namespace: string,
  name: string,
): Promise<SandboxStatus> {
  const result = await clients.custom.getNamespacedCustomObject({
    group: SANDBOX_GROUP,
    version: SANDBOX_VERSION,
    namespace,
    plural: SANDBOX_PLURAL,
    name,
  });
  return mapSandboxStatus(result as Record<string, unknown>);
}

/**
 * Returns the pod name backing a Sandbox CR. v1beta1 exposes the controller's
 * exact pod label selector as status.selector (the selector uses a hash label),
 * and the controller-owned Pod name normally matches the Sandbox name. Every
 * result must carry an ownerReference to this exact Sandbox UID before exec.
 */
async function readOwnedSandboxPod(
  clients: KubeClients,
  namespace: string,
  name: string,
  expectedSandboxUid?: string,
): Promise<Record<string, unknown> | null> {
  // Read the CR once for its v1beta1 selector and UID before resolving a Pod.
  const cr = await clients.custom.getNamespacedCustomObject({
    group: SANDBOX_GROUP,
    version: SANDBOX_VERSION,
    namespace,
    plural: SANDBOX_PLURAL,
    name,
  }) as Record<string, unknown>;

  const status = (cr.status as Record<string, unknown>) ?? {};
  const crMetadata = (cr.metadata as Record<string, unknown>) ?? {};
  const sandboxUid =
    typeof crMetadata.uid === "string" && crMetadata.uid.trim()
      ? crMetadata.uid
      : undefined;
  if (!sandboxUid) {
    if (expectedSandboxUid) {
      throw new SandboxIdentityMismatchError(
        `Sandbox ${namespace}/${name} has no UID (expected Sandbox UID ${expectedSandboxUid}); reacquire the Kubernetes lease.`,
      );
    }
    return null;
  }
  if (expectedSandboxUid && sandboxUid !== expectedSandboxUid) {
    throw new SandboxIdentityMismatchError(
      `Sandbox ${namespace}/${name} was replaced (expected Sandbox UID ${expectedSandboxUid}, found ${sandboxUid}); reacquire the Kubernetes lease.`,
    );
  }
  const isOwnedPod = (pod: Record<string, unknown>): boolean => {
    const podMetadata = (pod.metadata as Record<string, unknown>) ?? {};
    const ownerReferences = Array.isArray(podMetadata.ownerReferences)
      ? (podMetadata.ownerReferences as Array<Record<string, unknown>>)
      : [];
    return ownerReferences.some((owner) =>
      owner.apiVersion === SANDBOX_API_VERSION &&
      owner.kind === "Sandbox" &&
      owner.name === name &&
      owner.controller === true &&
      owner.uid === sandboxUid,
    );
  };
  const readOwnedPod = async (
    podName: string,
  ): Promise<Record<string, unknown> | null> => {
    try {
      const pod = (await clients.core.readNamespacedPod({
        namespace,
        name: podName,
      })) as Record<string, unknown>;
      return isOwnedPod(pod) ? pod : null;
    } catch (err) {
      const code =
        (err as { code?: number; statusCode?: number }).code ??
        (err as { code?: number; statusCode?: number }).statusCode;
      if (code === 404) return null;
      throw err;
    }
  };

  // Warm-pool controllers can report a Pod name in this annotation. Otherwise,
  // the standard controller names the Pod after the Sandbox.
  const annotations =
    (crMetadata.annotations as Record<string, unknown> | undefined) ?? {};
  const annotatedPodName = annotations["agents.x-k8s.io/pod-name"];
  const candidateNames = [
    ...(typeof annotatedPodName === "string" && annotatedPodName.trim()
      ? [annotatedPodName.trim()]
      : []),
    name,
  ];
  const exactMatches: Record<string, unknown>[] = [];
  for (const candidate of new Set(candidateNames)) {
    const pod = await readOwnedPod(candidate);
    if (pod) exactMatches.push(pod);
  }
  const runningExact = exactMatches.find((pod) => {
    const podStatus = (pod.status as Record<string, unknown>) ?? {};
    return podStatus.phase === "Running";
  });
  const exact = runningExact ?? exactMatches[0];
  if (exact) return exact;

  // v1beta1's selector is the controller-written sandbox-name-hash selector;
  // it avoids relying on the removed full-name sandbox label or name prefixes.
  const selector = status.selector;
  const selectorMatch =
    typeof selector === "string"
      ? /^agents\.x-k8s\.io\/sandbox-name-hash=([A-Za-z0-9_.-]+)$/.exec(selector)
      : null;
  if (!selectorMatch) return null;

  const result = await clients.core.listNamespacedPod({
    namespace,
    labelSelector: selector as string,
  });
  const items =
    (
      (
        result as {
          items?: {
            metadata?: { name?: string; labels?: Record<string, string> };
            status?: { phase?: string };
          }[];
        }
      ).items
    ) ?? [];

  // Keep defensive checks for both the status selector and the exact CR UID.
  const matching = items.filter(
    (pod) =>
      (pod.metadata?.labels ?? {})["agents.x-k8s.io/sandbox-name-hash"] ===
        selectorMatch[1] &&
      isOwnedPod(pod as unknown as Record<string, unknown>),
  );

  const running = matching.find((p) => p.status?.phase === "Running");
  const found = running ?? matching[0];
  return found ? found as unknown as Record<string, unknown> : null;
}

/**
 * Read the currently owned Sandbox pod identity. When expectedSandboxUid is
 * supplied, a same-name replacement Sandbox is rejected before its pod can be
 * considered.
 */
export async function getSandboxPodIdentity(
  clients: KubeClients,
  namespace: string,
  name: string,
  expectedSandboxUid?: string,
): Promise<SandboxPodIdentity | null> {
  const pod = await readOwnedSandboxPod(clients, namespace, name, expectedSandboxUid);
  if (!pod) return null;
  const metadata = (pod.metadata as Record<string, unknown>) ?? {};
  const podName = metadata.name;
  const podUid = metadata.uid;
  if (typeof podName !== "string" || !podName.trim()) return null;
  if (typeof podUid !== "string" || !podUid.trim()) return null;
  const status = (pod.status as Record<string, unknown>) ?? {};
  return {
    name: podName,
    uid: podUid,
    terminating: Boolean(metadata.deletionTimestamp),
    ...(typeof status.phase === "string" ? { phase: status.phase } : {}),
  };
}

export async function getSandboxCrShutdownTime(
  clients: KubeClients,
  namespace: string,
  name: string,
  expectedSandboxUid: string,
): Promise<string | null> {
  const cr = await clients.custom.getNamespacedCustomObject({
    group: SANDBOX_GROUP,
    version: SANDBOX_VERSION,
    namespace,
    plural: SANDBOX_PLURAL,
    name,
  }) as Record<string, unknown>;
  const metadata = (cr.metadata as Record<string, unknown>) ?? {};
  const currentUid = metadata.uid;
  if (currentUid !== expectedSandboxUid) {
    throw new SandboxIdentityMismatchError(
      `Sandbox ${namespace}/${name} was replaced (expected Sandbox UID ${expectedSandboxUid}, found ${String(currentUid ?? "missing")}); reacquire the Kubernetes lease.`,
    );
  }
  const spec = (cr.spec as Record<string, unknown>) ?? {};
  return spec.shutdownPolicy === "Delete" && typeof spec.shutdownTime === "string"
    ? spec.shutdownTime
    : null;
}

export async function assertSandboxCrUid(
  clients: KubeClients,
  namespace: string,
  name: string,
  expectedSandboxUid: string,
): Promise<void> {
  const cr = await clients.custom.getNamespacedCustomObject({
    group: SANDBOX_GROUP,
    version: SANDBOX_VERSION,
    namespace,
    plural: SANDBOX_PLURAL,
    name,
  }) as Record<string, unknown>;
  const metadata = (cr.metadata as Record<string, unknown>) ?? {};
  if (metadata.uid !== expectedSandboxUid) {
    throw new SandboxIdentityMismatchError(
      `Sandbox ${namespace}/${name} was replaced (expected Sandbox UID ${expectedSandboxUid}, found ${String(metadata.uid ?? "missing")}); reacquire the Kubernetes lease.`,
    );
  }
}

/** Wait only for an owned Pod identity, not controller Ready or image startup. */
export async function waitForSandboxPodIdentity(
  clients: KubeClients,
  namespace: string,
  name: string,
  expectedSandboxUid: string,
  opts: { timeoutMs?: number; pollMs?: number } = {},
): Promise<SandboxPodIdentity> {
  const timeoutMs = opts.timeoutMs ?? 10_000;
  const pollMs = opts.pollMs ?? 200;
  const deadline = Date.now() + timeoutMs;
  while (true) {
    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0) break;
    let timeout: ReturnType<typeof setTimeout> | undefined;
    const identityOrTimeout = await Promise.race([
      getSandboxPodIdentity(clients, namespace, name, expectedSandboxUid),
      new Promise<null>((resolve) => {
        timeout = setTimeout(() => resolve(null), remainingMs);
      }),
    ]).finally(() => {
      if (timeout) clearTimeout(timeout);
    });
    const identity = identityOrTimeout;
    if (identity && !identity.terminating) return identity;
    const afterReadMs = deadline - Date.now();
    if (afterReadMs <= 0) break;
    await sleep(Math.min(pollMs, afterReadMs));
  }
  throw new SandboxPodIdentityTimeoutError(namespace, name, timeoutMs);
}

export async function findPodForSandbox(
  clients: KubeClients,
  namespace: string,
  name: string,
  expectedSandboxUid?: string,
): Promise<string | null> {
  const pod = await readOwnedSandboxPod(clients, namespace, name, expectedSandboxUid);
  const podName = ((pod?.metadata as Record<string, unknown>) ?? {}).name;
  return typeof podName === "string" ? podName : null;
}

export async function streamSandboxLogs(
  clients: KubeClients,
  namespace: string,
  podName: string,
  onChunk: (stream: "stdout" | "stderr", text: string) => Promise<void>,
): Promise<void> {
  // V1 limitation: readNamespacedPodLog returns combined stdout. The
  // sleep-infinity pod will have minimal output; this is provided for interface
  // conformance. For actual command output, use execInPod() directly.
  const result = await clients.core.readNamespacedPodLog({
    namespace,
    name: podName,
  });
  const text = (result as string) ?? "";
  if (text.length > 0) await onChunk("stdout", text);
}

export async function deleteSandboxCr(
  clients: KubeClients,
  namespace: string,
  name: string,
): Promise<void> {
  await clients.custom.deleteNamespacedCustomObject({
    group: SANDBOX_GROUP,
    version: SANDBOX_VERSION,
    namespace,
    plural: SANDBOX_PLURAL,
    name,
    propagationPolicy: "Foreground",
  });
}

/** Delete only the exact Sandbox object backing a persisted lease. */
export async function deleteSandboxCrIfUid(
  clients: KubeClients,
  namespace: string,
  name: string,
  expectedSandboxUid: string,
): Promise<void> {
  const cr = await clients.custom.getNamespacedCustomObject({
    group: SANDBOX_GROUP,
    version: SANDBOX_VERSION,
    namespace,
    plural: SANDBOX_PLURAL,
    name,
  }) as Record<string, unknown>;
  const metadata = (cr.metadata as Record<string, unknown>) ?? {};
  if (metadata.uid !== expectedSandboxUid) {
    throw new SandboxIdentityMismatchError(
      `Sandbox ${namespace}/${name} was replaced (expected Sandbox UID ${expectedSandboxUid}, found ${String(metadata.uid ?? "missing")}); refusing to delete the replacement.`,
    );
  }
  await clients.custom.deleteNamespacedCustomObject({
    group: SANDBOX_GROUP,
    version: SANDBOX_VERSION,
    namespace,
    plural: SANDBOX_PLURAL,
    name,
    propagationPolicy: "Foreground",
    body: { preconditions: { uid: expectedSandboxUid } },
  });
}

/**
 * Wait until the Sandbox CR's pod reaches Ready phase (i.e., the pod is up and
 * exec-able). This is NOT waiting for a workload to finish — the Sandbox pod
 * runs sleep infinity indefinitely. Execution completion is tracked by the
 * individual execInPod() calls.
 *
 * Throws SandboxCrTimeoutError if Ready is not reached within timeoutMs.
 * Throws if the Sandbox transitions to Failed.
 */
export async function waitForSandboxReady(
  clients: KubeClients,
  namespace: string,
  name: string,
  opts: { timeoutMs: number; pollMs?: number } = {
    timeoutMs: 120_000,
    pollMs: 2000,
  },
): Promise<SandboxStatus> {
  const deadline = Date.now() + opts.timeoutMs;
  const pollMs = opts.pollMs ?? 2000;

  while (Date.now() < deadline) {
    const cr = await clients.custom.getNamespacedCustomObject({
      group: SANDBOX_GROUP,
      version: SANDBOX_VERSION,
      namespace,
      plural: SANDBOX_PLURAL,
      name,
    }) as Record<string, unknown>;

    const conditions = getConditions(cr);
    const terminal = getTerminalStatus(conditions, cr);
    if (terminal) {
      throw new Error(
        `Sandbox ${namespace}/${name} failed: ${terminal.reason ?? "unknown reason"} — ${terminal.message ?? ""}`,
      );
    }
    const metadata = (cr.metadata as Record<string, unknown>) ?? {};
    if (metadata.deletionTimestamp) {
      // A Sandbox being torn down will never transition to Ready. Polling
      // until the deadline would burn the full timeoutMs (potentially
      // 30+ minutes) before throwing a generic timeout. Fail fast instead
      // so the caller can surface a clear "the lease is being released"
      // error and decide whether to retry against a fresh Sandbox.
      throw new Error(
        `Sandbox ${namespace}/${name} is terminating — cannot wait for Ready`,
      );
    }
    const suspended = getCondition(conditions, "Suspended");
    if (suspended?.status === "True" && isConditionCurrent(suspended, cr)) {
      throw new Error(
        `Sandbox ${namespace}/${name} is suspended — cannot wait for Ready`,
      );
    }
    const readyCondition = getCondition(conditions, "Ready");
    if (
      readyCondition?.status === "True" &&
      isConditionCurrent(readyCondition, cr)
    ) {
      return mapSandboxStatus(cr);
    }
    // Pending — keep polling
    await sleep(pollMs);
  }

  throw new SandboxCrTimeoutError(namespace, name, opts.timeoutMs);
}

/**
 * Sandbox CR-backed conformance to SandboxOrchestrator.
 *
 * waitForCompletion semantics change: for this backend, "completion" means
 * "pod is up and Ready to exec into" — NOT "workload finished". The actual
 * command execution and its completion is handled by execInPod().
 */
export const sandboxCrOrchestrator: SandboxOrchestrator = {
  claim: createSandboxCr,
  getStatus: getSandboxCrStatus,
  findPod: findPodForSandbox,
  streamLogs: streamSandboxLogs,
  release: deleteSandboxCr,
  waitForCompletion: waitForSandboxReady,
};
