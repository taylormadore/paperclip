/**
 * Resume + destroy lifecycle helpers for Kubernetes sandbox leases.
 *
 * Resume semantics: a lease is resumable only while its workload resource
 * (Sandbox CR or Job) still exists and its pod is Running/Ready (or becomes
 * Ready within a short bounded wait). Unlike Daytona — where a stopped sandbox
 * can be started again by ID — Kubernetes pods are NOT restartable: once the
 * pod backing a lease is gone or terminally failed, the lease can never be
 * revived in place. That asymmetry is intentional; the plugin reports the
 * lease as expired and the server falls back to a fresh acquireLease, which
 * provisions a new pod.
 *
 * Destroy semantics: the forced cleanup path. Deletes every resource
 * acquireLease created (Sandbox CR / Job, its pod, the per-run Secret),
 * treating 404s as success so it is idempotent and safe to call against
 * half-deleted leases.
 */

import type { KubeClients } from "./kube-client.js";
import { deleteJob, findPodForJob, getJobStatus } from "./job-orchestrator.js";
import {
  deleteSandboxCr,
  deleteSandboxCrIfUid,
  assertSandboxCrUid,
  getSandboxCrShutdownTime,
  getSandboxPodIdentity,
  SandboxIdentityMismatchError,
  waitForSandboxReady,
} from "./sandbox-cr-orchestrator.js";

/** True when a Kubernetes API error means "resource not found" (HTTP 404). */
export function isKubeNotFoundError(err: unknown): boolean {
  const code = (err as { code?: number; statusCode?: number }).code
    ?? (err as { code?: number; statusCode?: number }).statusCode;
  return code === 404;
}

async function ignoreNotFound(promise: Promise<unknown>): Promise<void> {
  try {
    await promise;
  } catch (err) {
    if (!isKubeNotFoundError(err)) throw err;
  }
}

export function isKubeUidPreconditionConflictError(err: unknown): boolean {
  const code = (err as { code?: number; statusCode?: number }).code
    ?? (err as { code?: number; statusCode?: number }).statusCode;
  if (code !== 409) return false;
  const body = (err as { body?: unknown }).body;
  const bodyText = typeof body === "string" ? body : JSON.stringify(body ?? "");
  const message = (err as { message?: unknown }).message;
  const detailText = [bodyText, typeof message === "string" ? message : ""].join(" ");
  return /uid.{0,100}precondition|precondition.{0,100}uid/i.test(detailText);
}

async function ignoreNotFoundOrUidConflict(promise: Promise<unknown>): Promise<void> {
  try {
    await promise;
  } catch (err) {
    if (!isKubeNotFoundError(err) && !isKubeUidPreconditionConflictError(err)) throw err;
  }
}

export type ResumeCheckResult =
  | {
      resumable: true;
      podName: string | null;
      phase: "Pending" | "Running";
      sandboxUid?: string;
      podUid?: string;
      expiresAt?: string | null;
    }
  | { resumable: false; reason: string };

export interface ResumeCheckInput {
  namespace: string;
  /** Workload resource name (Sandbox CR name or Job name) == providerLeaseId. */
  name: string;
  backend: "sandbox-cr" | "job";
  /** Bounded wait for an existing Sandbox pod to report Ready. */
  readyTimeoutMs?: number;
  pollMs?: number;
  /** Persisted Sandbox-CR identity. Required for sandbox-cr resume. */
  expectedSandboxUid?: string;
  expectedPodUid?: string;
}

/**
 * Check whether the workload behind a lease is still alive and exec-able.
 * Returns `resumable: false` (never throws "expected" states) when the
 * resource is gone (404), terminally failed, terminating, or doesn't become
 * Ready within the bounded wait — all of which mean the caller should fall
 * back to a fresh acquireLease.
 */
export async function checkLeaseResumable(
  clients: KubeClients,
  input: ResumeCheckInput,
): Promise<ResumeCheckResult> {
  if (input.backend === "sandbox-cr") {
    if (!input.expectedSandboxUid || !input.expectedPodUid) {
      return {
        resumable: false,
        reason: "Sandbox lease is missing its persisted Sandbox/Pod UIDs; reacquire the lease.",
      };
    }

    // Bounded wait for the Sandbox to report Ready. waitForSandboxReady fails
    // fast on Failed/Terminating; a timeout means the pod never came up. None
    // of those states are resumable — k8s pods cannot be restarted in place.
    try {
      // Check identity before readiness so a current Ready condition on a
      // same-name replacement cannot make the old lease appear resumable.
      await assertSandboxCrUid(
        clients,
        input.namespace,
        input.name,
        input.expectedSandboxUid,
      );
      await waitForSandboxReady(clients, input.namespace, input.name, {
        timeoutMs: input.readyTimeoutMs ?? 30_000,
        pollMs: input.pollMs ?? 1_000,
      });
    } catch (err) {
      if (isKubeNotFoundError(err)) {
        return { resumable: false, reason: "Sandbox CR no longer exists" };
      }
      if (err instanceof SandboxIdentityMismatchError) {
        return { resumable: false, reason: err.message };
      }
      return {
        resumable: false,
        reason: err instanceof Error ? err.message : String(err),
      };
    }

    let podName: string | null;
    try {
      const identity = await getSandboxPodIdentity(
        clients,
        input.namespace,
        input.name,
        input.expectedSandboxUid,
      );
      if (!identity) {
        return {
          resumable: false,
          reason: "Sandbox is Ready but no owned Pod with a UID was found",
        };
      }
      if (identity.uid !== input.expectedPodUid) {
        return {
          resumable: false,
          reason: `Pod ${identity.name} was recreated (expected Pod UID ${input.expectedPodUid}, found ${identity.uid})`,
        };
      }
      if (identity.phase !== "Running" || identity.terminating) {
        return {
          resumable: false,
          reason: `Pod ${identity.name} is ${identity.terminating ? "terminating" : identity.phase ?? "in an unknown phase"}`,
        };
      }
      podName = identity.name;
    } catch (err) {
      // CR deleted between the readiness check and the pod lookup.
      if (isKubeNotFoundError(err)) {
        return { resumable: false, reason: "Sandbox CR no longer exists" };
      }
      if (err instanceof SandboxIdentityMismatchError) {
        return { resumable: false, reason: err.message };
      }
      throw err;
    }

    let expiresAt: string | null;
    try {
      expiresAt = await getSandboxCrShutdownTime(
        clients,
        input.namespace,
        input.name,
        input.expectedSandboxUid,
      );
    } catch (err) {
      if (isKubeNotFoundError(err)) {
        return { resumable: false, reason: "Sandbox CR no longer exists" };
      }
      if (err instanceof SandboxIdentityMismatchError) {
        return { resumable: false, reason: err.message };
      }
      throw err;
    }
    const expiryMs = expiresAt === null ? Number.NaN : Date.parse(expiresAt);
    if (!Number.isFinite(expiryMs)) {
      return {
        resumable: false,
        reason: "Sandbox CR has no valid shutdownTime with shutdownPolicy Delete; reacquire the lease.",
      };
    }
    if (expiryMs <= Date.now()) {
      return {
        resumable: false,
        reason: "Sandbox CR shutdownTime has expired; reacquire the lease.",
      };
    }

    // Confirm the pod still has the exact UID immediately after identity and
    // expiry reads — the CR status can lag pod deletion/recreation.
    let pod: { metadata?: { uid?: string; deletionTimestamp?: unknown }; status?: { phase?: string } };
    try {
      pod = await clients.core.readNamespacedPod({
        namespace: input.namespace,
        name: podName,
      }) as typeof pod;
    } catch (err) {
      if (isKubeNotFoundError(err)) {
        return { resumable: false, reason: `Pod ${podName} no longer exists` };
      }
      throw err;
    }
    const podPhase = pod.status?.phase;
    const terminating = Boolean(pod.metadata?.deletionTimestamp);
    if (pod.metadata?.uid !== input.expectedPodUid) {
      return {
        resumable: false,
        reason: `Pod ${podName} was recreated (expected Pod UID ${input.expectedPodUid}, found ${pod.metadata?.uid ?? "missing"})`,
      };
    }
    if (podPhase !== "Running" || terminating) {
      return {
        resumable: false,
        reason: `Pod ${podName} is ${terminating ? "terminating" : podPhase ?? "in an unknown phase"}`,
      };
    }
    return {
      resumable: true,
      podName,
      phase: "Running",
      sandboxUid: input.expectedSandboxUid,
      podUid: input.expectedPodUid,
      expiresAt,
    };
  }

  // ── Job backend ───────────────────────────────────────────────────────────
  let status;
  try {
    status = await getJobStatus(clients, input.namespace, input.name);
  } catch (err) {
    if (isKubeNotFoundError(err)) {
      return { resumable: false, reason: "Job no longer exists" };
    }
    throw err;
  }
  if (status.phase === "Succeeded" || status.phase === "Failed") {
    // Terminal Jobs cannot be re-run in place.
    return { resumable: false, reason: `Job is ${status.phase}` };
  }
  // Pending/Running Jobs are resumable: execute() waits for completion
  // itself, so a not-yet-scheduled pod (podName null) is fine here.
  const podName = await findPodForJob(clients, input.namespace, input.name);
  return {
    resumable: true,
    podName,
    phase: status.phase === "Running" ? "Running" : "Pending",
  };
}

export interface DestroyLeaseInput {
  namespace: string;
  /** Workload resource name (Sandbox CR name or Job name) == providerLeaseId. */
  name: string;
  backend: "sandbox-cr" | "job";
  podName: string | null;
  secretName: string | null;
  /** Guard same-name Sandbox replacements during partial-acquire cleanup. */
  expectedSandboxUid?: string;
  /** When known, delete only the Pod captured by the lease. */
  expectedPodUid?: string;
  /** When known, delete only the Secret captured by the lease. */
  expectedSecretUid?: string;
}

/**
 * Forcibly delete every resource acquireLease created for this lease.
 * Workload first (its deletion cascades to the pod and, via ownerReferences,
 * the per-run Secret in the normal case); then the pod and Secret explicitly
 * so a wedged controller or broken ownerRef cannot strand them. Deletes treat
 * 404 as success; UID-precondition conflicts mean a same-name replacement won
 * the race and are also safe no-ops for the old lease.
 */
export async function destroyLeaseResources(
  clients: KubeClients,
  input: DestroyLeaseInput,
): Promise<void> {
  if (input.backend === "sandbox-cr") {
    if (input.expectedSandboxUid) {
      // The UID precondition closes the get/delete race if the CR is replaced.
      try {
        await deleteSandboxCrIfUid(
          clients,
          input.namespace,
          input.name,
          input.expectedSandboxUid,
        );
      } catch (err) {
        if (
          !isKubeNotFoundError(err) &&
          !isKubeUidPreconditionConflictError(err) &&
          !(err instanceof SandboxIdentityMismatchError)
        ) {
          throw err;
        }
      }
    } else {
      await ignoreNotFound(deleteSandboxCr(clients, input.namespace, input.name));
    }
  } else {
    await ignoreNotFound(deleteJob(clients, input.namespace, input.name));
  }
  if (input.podName) {
    if (input.backend === "sandbox-cr" && input.expectedSandboxUid) {
      type CleanupPod = {
        metadata?: {
          uid?: unknown;
          ownerReferences?: Array<Record<string, unknown>>;
        };
      };
      let pod: CleanupPod | null = null;
      try {
        pod = await clients.core.readNamespacedPod({
          namespace: input.namespace,
          name: input.podName,
        }) as unknown as CleanupPod;
      } catch (err) {
        if (isKubeNotFoundError(err)) pod = null;
        else throw err;
      }
      const ownerReferences = pod?.metadata?.ownerReferences ?? [];
      const isExpectedOwnedPod = ownerReferences.some((owner) =>
        owner.apiVersion === "agents.x-k8s.io/v1beta1" &&
        owner.kind === "Sandbox" &&
        owner.name === input.name &&
        owner.uid === input.expectedSandboxUid &&
        owner.controller === true,
      );
      const podUid = pod?.metadata?.uid;
      const podUidMatches = !input.expectedPodUid || podUid === input.expectedPodUid;
      if (isExpectedOwnedPod && typeof podUid === "string" && podUidMatches) {
        await ignoreNotFoundOrUidConflict(
          clients.core.deleteNamespacedPod({
            namespace: input.namespace,
            name: input.podName,
            body: { preconditions: { uid: podUid } },
          }),
        );
      }
    } else {
      await ignoreNotFound(
        clients.core.deleteNamespacedPod({
          namespace: input.namespace,
          name: input.podName,
        }),
      );
    }
  }
  if (input.secretName) {
    if (input.backend === "sandbox-cr" && input.expectedSandboxUid) {
      // New leases persist this UID from createNamespacedSecret's response.
      // Without it, deleting the Sandbox cascades to its Secret, so no Secret
      // GET permission is needed during cleanup.
      if (input.expectedSecretUid) {
        await ignoreNotFoundOrUidConflict(
          clients.core.deleteNamespacedSecret({
            namespace: input.namespace,
            name: input.secretName,
            body: { preconditions: { uid: input.expectedSecretUid } },
          }),
        );
      }
    } else {
      await ignoreNotFound(
        clients.core.deleteNamespacedSecret({
          namespace: input.namespace,
          name: input.secretName,
        }),
      );
    }
  }
}
