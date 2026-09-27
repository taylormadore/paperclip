import { describe, it, expect, vi } from "vitest";
import {
  checkLeaseResumable,
  destroyLeaseResources,
  isKubeNotFoundError,
} from "../../src/lease-lifecycle.js";

const SANDBOX_GROUP = "agents.x-k8s.io";
const SANDBOX_VERSION = "v1beta1";
const SANDBOX_API_VERSION = `${SANDBOX_GROUP}/${SANDBOX_VERSION}`;
const SANDBOX_PLURAL = "sandboxes";

function notFound(): Error {
  return Object.assign(new Error("not found"), { code: 404 });
}

function readySandboxCr(podName?: string): Record<string, unknown> {
  return {
    metadata: {
      uid: "uid-1",
      generation: 1,
      ...(podName ? { annotations: { "agents.x-k8s.io/pod-name": podName } } : {}),
    },
    status: {
      conditions: [
        {
          type: "Ready",
          status: "True",
          reason: "DependenciesReady",
          observedGeneration: 1,
        },
      ],
      selector: "agents.x-k8s.io/sandbox-name-hash=1a2b3c",
    },
  };
}

function sandboxOwnedPod(name: string, extraMetadata: Record<string, unknown> = {}) {
  return {
    metadata: {
      name,
      ...extraMetadata,
      ownerReferences: [
        {
          apiVersion: SANDBOX_API_VERSION,
          kind: "Sandbox",
          name: "pc-abc",
          uid: "uid-1",
          controller: true,
        },
      ],
    },
    status: { phase: "Running" },
  };
}

describe("isKubeNotFoundError", () => {
  it("matches code=404 and statusCode=404", () => {
    expect(isKubeNotFoundError({ code: 404 })).toBe(true);
    expect(isKubeNotFoundError({ statusCode: 404 })).toBe(true);
  });

  it("does not match other errors", () => {
    expect(isKubeNotFoundError({ code: 500 })).toBe(false);
    expect(isKubeNotFoundError(new Error("boom"))).toBe(false);
  });
});

describe("checkLeaseResumable (sandbox-cr backend)", () => {
  it("resumes a live lease whose Sandbox is Ready and pod is Running", async () => {
    const clients = {
      custom: {
        getNamespacedCustomObject: vi.fn().mockResolvedValue(readySandboxCr("pc-abc-pod")),
      },
      core: {
        readNamespacedPod: vi.fn().mockResolvedValue(sandboxOwnedPod("pc-abc-pod")),
      },
    };
    const result = await checkLeaseResumable(clients as never, {
      namespace: "paperclip-acme",
      name: "pc-abc",
      backend: "sandbox-cr",
      readyTimeoutMs: 1_000,
      pollMs: 10,
    });
    expect(result).toEqual({ resumable: true, podName: "pc-abc-pod", phase: "Running" });
    expect(clients.core.readNamespacedPod).toHaveBeenCalledWith({
      namespace: "paperclip-acme",
      name: "pc-abc-pod",
    });
  });

  it("is not resumable when the Sandbox CR is gone (404)", async () => {
    const clients = {
      custom: { getNamespacedCustomObject: vi.fn().mockRejectedValue(notFound()) },
      core: { readNamespacedPod: vi.fn() },
    };
    const result = await checkLeaseResumable(clients as never, {
      namespace: "ns",
      name: "pc-abc",
      backend: "sandbox-cr",
      readyTimeoutMs: 1_000,
      pollMs: 10,
    });
    expect(result.resumable).toBe(false);
    if (!result.resumable) expect(result.reason).toMatch(/no longer exists/);
  });

  it("is not resumable when the Sandbox is terminally Failed", async () => {
    const clients = {
      custom: {
        getNamespacedCustomObject: vi.fn().mockResolvedValue({
          metadata: { uid: "uid-1", generation: 1 },
          status: {
            conditions: [
              {
                type: "Ready",
                status: "False",
                reason: "PodFailed",
                message: "no image",
                observedGeneration: 1,
              },
            ],
          },
        }),
      },
      core: { readNamespacedPod: vi.fn() },
    };
    const result = await checkLeaseResumable(clients as never, {
      namespace: "ns",
      name: "pc-abc",
      backend: "sandbox-cr",
      readyTimeoutMs: 1_000,
      pollMs: 10,
    });
    expect(result.resumable).toBe(false);
    if (!result.resumable) expect(result.reason).toMatch(/failed/i);
  });

  it("is not resumable when the Sandbox never reaches Ready within the bounded wait", async () => {
    const clients = {
      custom: {
        getNamespacedCustomObject: vi.fn().mockResolvedValue({
          metadata: { uid: "uid-1", generation: 1 },
          status: {
            conditions: [
              {
                type: "Ready",
                status: "False",
                reason: "DependenciesNotReady",
                observedGeneration: 1,
              },
            ],
          },
        }),
      },
      core: { readNamespacedPod: vi.fn() },
    };
    const result = await checkLeaseResumable(clients as never, {
      namespace: "ns",
      name: "pc-abc",
      backend: "sandbox-cr",
      readyTimeoutMs: 30,
      pollMs: 5,
    });
    expect(result.resumable).toBe(false);
    if (!result.resumable) expect(result.reason).toMatch(/did not reach .*Ready/);
  });

  it("is not resumable when the backing pod is gone (404)", async () => {
    const clients = {
      custom: {
        getNamespacedCustomObject: vi.fn().mockResolvedValue(readySandboxCr("pc-abc-pod")),
      },
      core: {
        readNamespacedPod: vi.fn().mockRejectedValue(notFound()),
        listNamespacedPod: vi.fn().mockResolvedValue({ items: [] }),
      },
    };
    const result = await checkLeaseResumable(clients as never, {
      namespace: "ns",
      name: "pc-abc",
      backend: "sandbox-cr",
      readyTimeoutMs: 1_000,
      pollMs: 10,
    });
    expect(result.resumable).toBe(false);
    if (!result.resumable) expect(result.reason).toMatch(/no backing pod was found/);
  });

  it("is not resumable when the pod is being torn down (deletionTimestamp set)", async () => {
    const clients = {
      custom: {
        getNamespacedCustomObject: vi.fn().mockResolvedValue(readySandboxCr("pc-abc-pod")),
      },
      core: {
        readNamespacedPod: vi.fn().mockResolvedValue(
          sandboxOwnedPod("pc-abc-pod", {
            deletionTimestamp: "2026-06-10T00:00:00Z",
          }),
        ),
      },
    };
    const result = await checkLeaseResumable(clients as never, {
      namespace: "ns",
      name: "pc-abc",
      backend: "sandbox-cr",
      readyTimeoutMs: 1_000,
      pollMs: 10,
    });
    expect(result.resumable).toBe(false);
    if (!result.resumable) expect(result.reason).toMatch(/terminating/);
  });

  it("rethrows unexpected (non-404) pod read errors", async () => {
    const clients = {
      custom: {
        getNamespacedCustomObject: vi.fn().mockResolvedValue(readySandboxCr("pc-abc-pod")),
      },
      core: {
        readNamespacedPod: vi
          .fn()
          .mockRejectedValue(Object.assign(new Error("forbidden"), { code: 403 })),
      },
    };
    await expect(
      checkLeaseResumable(clients as never, {
        namespace: "ns",
        name: "pc-abc",
        backend: "sandbox-cr",
        readyTimeoutMs: 1_000,
        pollMs: 10,
      }),
    ).rejects.toThrow("forbidden");
  });
});

describe("checkLeaseResumable (job backend)", () => {
  it("resumes a Running Job lease", async () => {
    const clients = {
      batch: {
        readNamespacedJobStatus: vi.fn().mockResolvedValue({ status: { active: 1 } }),
      },
      core: {
        listNamespacedPod: vi.fn().mockResolvedValue({
          items: [{ metadata: { name: "pc-job-pod" }, status: { phase: "Running" } }],
        }),
      },
    };
    const result = await checkLeaseResumable(clients as never, {
      namespace: "ns",
      name: "pc-job",
      backend: "job",
    });
    expect(result).toEqual({ resumable: true, podName: "pc-job-pod", phase: "Running" });
  });

  it("is not resumable when the Job is gone (404)", async () => {
    const clients = {
      batch: { readNamespacedJobStatus: vi.fn().mockRejectedValue(notFound()) },
      core: { listNamespacedPod: vi.fn() },
    };
    const result = await checkLeaseResumable(clients as never, {
      namespace: "ns",
      name: "pc-job",
      backend: "job",
    });
    expect(result.resumable).toBe(false);
    if (!result.resumable) expect(result.reason).toMatch(/no longer exists/);
  });

  it("is not resumable when the Job already finished (terminal phase)", async () => {
    const clients = {
      batch: {
        readNamespacedJobStatus: vi.fn().mockResolvedValue({
          status: { succeeded: 1, conditions: [{ type: "Complete", status: "True" }] },
        }),
      },
      core: { listNamespacedPod: vi.fn() },
    };
    const result = await checkLeaseResumable(clients as never, {
      namespace: "ns",
      name: "pc-job",
      backend: "job",
    });
    expect(result.resumable).toBe(false);
    if (!result.resumable) expect(result.reason).toMatch(/Succeeded/);
  });
});

describe("destroyLeaseResources", () => {
  function makeClients() {
    return {
      custom: { deleteNamespacedCustomObject: vi.fn().mockResolvedValue({}) },
      batch: { deleteNamespacedJob: vi.fn().mockResolvedValue({}) },
      core: {
        deleteNamespacedPod: vi.fn().mockResolvedValue({}),
        deleteNamespacedSecret: vi.fn().mockResolvedValue({}),
      },
    };
  }

  it("deletes the Sandbox CR, pod, and per-run Secret (sandbox-cr backend)", async () => {
    const clients = makeClients();
    await destroyLeaseResources(clients as never, {
      namespace: "paperclip-acme",
      name: "pc-abc",
      backend: "sandbox-cr",
      podName: "pc-abc-pod",
      secretName: "pc-abc-env",
    });
    expect(clients.custom.deleteNamespacedCustomObject).toHaveBeenCalledWith({
      group: SANDBOX_GROUP,
      version: SANDBOX_VERSION,
      namespace: "paperclip-acme",
      plural: SANDBOX_PLURAL,
      name: "pc-abc",
      propagationPolicy: "Foreground",
    });
    expect(clients.core.deleteNamespacedPod).toHaveBeenCalledWith({
      namespace: "paperclip-acme",
      name: "pc-abc-pod",
    });
    expect(clients.core.deleteNamespacedSecret).toHaveBeenCalledWith({
      namespace: "paperclip-acme",
      name: "pc-abc-env",
    });
    expect(clients.batch.deleteNamespacedJob).not.toHaveBeenCalled();
  });

  it("deletes the Job instead of the Sandbox CR (job backend)", async () => {
    const clients = makeClients();
    await destroyLeaseResources(clients as never, {
      namespace: "ns",
      name: "pc-job",
      backend: "job",
      podName: null,
      secretName: "pc-job-env",
    });
    expect(clients.batch.deleteNamespacedJob).toHaveBeenCalledWith({
      namespace: "ns",
      name: "pc-job",
      propagationPolicy: "Foreground",
    });
    expect(clients.custom.deleteNamespacedCustomObject).not.toHaveBeenCalled();
    expect(clients.core.deleteNamespacedPod).not.toHaveBeenCalled();
    expect(clients.core.deleteNamespacedSecret).toHaveBeenCalledWith({
      namespace: "ns",
      name: "pc-job-env",
    });
  });

  it("is idempotent: every 404 is treated as success", async () => {
    const clients = {
      custom: { deleteNamespacedCustomObject: vi.fn().mockRejectedValue(notFound()) },
      batch: { deleteNamespacedJob: vi.fn() },
      core: {
        deleteNamespacedPod: vi.fn().mockRejectedValue(notFound()),
        deleteNamespacedSecret: vi.fn().mockRejectedValue(notFound()),
      },
    };
    await expect(
      destroyLeaseResources(clients as never, {
        namespace: "ns",
        name: "pc-abc",
        backend: "sandbox-cr",
        podName: "pc-abc-pod",
        secretName: "pc-abc-env",
      }),
    ).resolves.toBeUndefined();
    expect(clients.core.deleteNamespacedSecret).toHaveBeenCalled();
  });

  it("rethrows unexpected (non-404) delete errors", async () => {
    const clients = {
      custom: {
        deleteNamespacedCustomObject: vi
          .fn()
          .mockRejectedValue(Object.assign(new Error("forbidden"), { code: 403 })),
      },
      batch: { deleteNamespacedJob: vi.fn() },
      core: { deleteNamespacedPod: vi.fn(), deleteNamespacedSecret: vi.fn() },
    };
    await expect(
      destroyLeaseResources(clients as never, {
        namespace: "ns",
        name: "pc-abc",
        backend: "sandbox-cr",
        podName: null,
        secretName: null,
      }),
    ).rejects.toThrow("forbidden");
  });
});
