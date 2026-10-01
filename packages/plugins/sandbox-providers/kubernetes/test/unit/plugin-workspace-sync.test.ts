import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({ clients: {} as Record<string, unknown> }));

vi.mock("../../src/kube-client.js", () => ({
  createKubeConfig: vi.fn(() => ({})),
  makeKubeClients: vi.fn(() => h.clients),
}));

vi.mock("@paperclipai/plugin-sdk", () => ({
  definePlugin: (definition: unknown) => ({ definition }),
}));

vi.mock("../../src/tenant-orchestrator.js", () => ({
  ensureTenant: vi.fn(async () => undefined),
}));

vi.mock("../../src/lease-lifecycle.js", () => ({
  checkLeaseResumable: vi.fn(),
  destroyLeaseResources: vi.fn(async () => undefined),
  isKubeUidPreconditionConflictError: vi.fn((err: unknown) => (err as { code?: number })?.code === 409),
  isKubeNotFoundError: vi.fn((err: unknown) => (err as { code?: number })?.code === 404),
}));

vi.mock("../../src/scoped-network-egress.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../src/scoped-network-egress.js")>();
  return {
    ...actual,
    createScopedNetworkEgressPolicyOrReleaseWorkload: vi.fn(async () => null),
  };
});

vi.mock("../../src/secret-manager.js", () => ({
  createPerRunSecret: vi.fn(async () => "secret-uid"),
}));

vi.mock("../../src/sandbox-cr-orchestrator.js", () => ({
  sandboxCrOrchestrator: {
    claim: vi.fn(async () => ({ uid: "sandbox-uid" })),
    findPod: vi.fn(async () => "sandbox-pod"),
    waitForCompletion: vi.fn(async () => undefined),
    release: vi.fn(async () => undefined),
  },
  getSandboxPodIdentity: vi.fn(async () => ({
    name: "sandbox-pod",
    uid: "pod-uid",
    phase: "Running",
    terminating: false,
  })),
  waitForSandboxPodIdentity: vi.fn(async () => ({
    name: "sandbox-pod",
    uid: "pod-uid",
    phase: "Pending",
    terminating: false,
  })),
  deleteSandboxCrIfUid: vi.fn(async () => undefined),
  SandboxIdentityMismatchError: class SandboxIdentityMismatchError extends Error {},
  SandboxPodIdentityTimeoutError: class SandboxPodIdentityTimeoutError extends Error {},
  SandboxCrTimeoutError: class SandboxCrTimeoutError extends Error {},
}));

vi.mock("../../src/pod-exec.js", () => ({
  execInPod: vi.fn(),
  execInPodStreaming: vi.fn(async () => ({ exitCode: 0, stderr: "" })),
  wrapCommandWithPodUid: vi.fn((command: string[], _env: unknown, podUid: string) => [
    "/bin/sh",
    "-c",
    "check-pod-uid-then-exec",
    "paperclip-sandbox-uid-guard",
    podUid,
    ...command,
  ]),
}));

import plugin from "../../src/plugin.js";
import { execInPodStreaming } from "../../src/pod-exec.js";
import {
  getSandboxPodIdentity,
  sandboxCrOrchestrator,
  SandboxPodIdentityTimeoutError,
  waitForSandboxPodIdentity,
} from "../../src/sandbox-cr-orchestrator.js";
import { createPerRunSecret } from "../../src/secret-manager.js";
import { destroyLeaseResources } from "../../src/lease-lifecycle.js";
import { createScopedNetworkEgressPolicyOrReleaseWorkload } from "../../src/scoped-network-egress.js";

const CONFIG = {
  inCluster: true,
  backend: "sandbox-cr",
  runtimeImage: "ghcr.io/paperclipai/agent:test",
};

beforeEach(() => {
  h.clients = {};
  vi.clearAllMocks();
});

describe("Kubernetes acquired workspace lease sync metadata", () => {
  it("carries the realized default workspace root through acquisition, realization, and sync", async () => {
    const lease = await plugin.definition.onEnvironmentAcquireLease!({
      driverKey: "kubernetes",
      companyId: "acme",
      environmentId: "env-1",
      config: CONFIG,
      runId: "run-1",
    });

    expect(lease.metadata?.remoteCwd).toBe("/workspace");
    expect(lease.metadata).toEqual(expect.objectContaining({
      sandboxUid: "sandbox-uid",
      podUid: "pod-uid",
      secretUid: "secret-uid",
    }));
    expect(lease.expiresAt).toBeTypeOf("string");
    expect(Date.parse(lease.expiresAt!) - Date.now()).toBeGreaterThan(23 * 60 * 60 * 1000);
    expect(Date.parse(lease.expiresAt!) - Date.now()).toBeLessThanOrEqual(24 * 60 * 60 * 1000);
    const [, , sandboxManifest] = vi.mocked(sandboxCrOrchestrator.claim).mock.calls[0]!;
    expect(sandboxManifest.spec.shutdownPolicy).toBe("Delete");
    expect(sandboxManifest.spec.shutdownTime).toBe(lease.expiresAt);

    const realized = await plugin.definition.onEnvironmentRealizeWorkspace!({
      driverKey: "kubernetes",
      companyId: "acme",
      environmentId: "env-1",
      config: CONFIG,
      lease,
      workspace: {},
    });

    expect(realized.cwd).toBe("/workspace");

    // Model the runner retaining the original acquired lease for its sync
    // closure, as the adapter-test route does. The hook still has the concrete
    // confinement root without relying on a rewritten lease object.
    const result = await plugin.definition.onEnvironmentSyncIn!({
      driverKey: "kubernetes",
      companyId: "acme",
      environmentId: "env-1",
      config: CONFIG,
      lease,
      operations: [
        {
          operationId: "sync-op",
          files: [],
          postUploadCommands: [{ command: "true" }],
        },
      ],
    });

    expect(result.operations).toEqual([
      { operationId: "sync-op", filesTransferred: 0, bytesTransferred: 0 },
    ]);
    expect(execInPodStreaming).toHaveBeenCalledOnce();
    const [, namespace, podName, , command] = vi.mocked(execInPodStreaming).mock.calls[0]!;
    expect(namespace).toBe("paperclip-acme");
    expect(podName).toBe("sandbox-pod");
    expect(command.join(" ")).toContain("'/workspace'");
  });

  it("captures an owned Pending Pod UID before acquire returns", async () => {
    vi.mocked(waitForSandboxPodIdentity).mockResolvedValueOnce({
      name: "sandbox-pod",
      uid: "pod-uid",
      phase: "Pending",
      terminating: false,
    });
    const lease = await plugin.definition.onEnvironmentAcquireLease!({
      driverKey: "kubernetes",
      companyId: "acme",
      environmentId: "env-1",
      config: CONFIG,
      runId: "run-pending",
    });
    expect(lease.metadata).toEqual(expect.objectContaining({
      sandboxUid: "sandbox-uid",
      podUid: "pod-uid",
      phase: "Pending",
    }));
  });

  it("cleans up the claimed Sandbox and rethrows the acquire error if Pod identity times out", async () => {
    const acquireError = new SandboxPodIdentityTimeoutError("paperclip-acme", "pc-timeout", 10_000);
    const cleanupError = new Error("cleanup API unavailable");
    vi.mocked(waitForSandboxPodIdentity).mockRejectedValueOnce(acquireError);
    vi.mocked(destroyLeaseResources).mockRejectedValueOnce(cleanupError);

    let thrown: unknown;
    try {
      await plugin.definition.onEnvironmentAcquireLease!({
        driverKey: "kubernetes",
        companyId: "acme",
        environmentId: "env-1",
        config: CONFIG,
        runId: "run-timeout",
      });
    } catch (err) {
      thrown = err;
    }

    expect(thrown).toBeInstanceOf(AggregateError);
    expect((thrown as AggregateError).errors).toEqual([acquireError, cleanupError]);
    expect((thrown as Error).message).toMatch(/cleanup was incomplete/);
    expect(vi.mocked(destroyLeaseResources)).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({
      backend: "sandbox-cr",
      expectedSandboxUid: "sandbox-uid",
      expectedSecretUid: "secret-uid",
      podName: null,
    }));
  });

  it("preserves a scoped-policy failure when its release already removed the Sandbox", async () => {
    const policyError = new Error("scoped policy create failed");
    vi.mocked(createScopedNetworkEgressPolicyOrReleaseWorkload).mockImplementationOnce(
      async (_input, releaseWorkload) => {
        await releaseWorkload();
        throw policyError;
      },
    );
    vi.mocked(sandboxCrOrchestrator.findPod).mockRejectedValueOnce(
      Object.assign(new Error("Sandbox already released"), { code: 404 }),
    );

    await expect(plugin.definition.onEnvironmentAcquireLease!({
      driverKey: "kubernetes",
      companyId: "acme",
      environmentId: "env-1",
      config: CONFIG,
      runId: "run-policy-cleanup",
    })).rejects.toBe(policyError);
    expect(destroyLeaseResources).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({
      expectedSandboxUid: "sandbox-uid",
      podName: null,
    }));
  });

  it("uses an earlier requested expiry and rejects a past explicit deadline before claiming", async () => {
    const requestedExpiresAt = new Date(Date.now() + 15 * 60 * 1000).toISOString();
    const lease = await plugin.definition.onEnvironmentAcquireLease!({
      driverKey: "kubernetes",
      companyId: "acme",
      environmentId: "env-1",
      config: CONFIG,
      runId: "run-shorter",
      requestedExpiresAt,
    });
    expect(lease.expiresAt).toBe(requestedExpiresAt);

    vi.mocked(sandboxCrOrchestrator.claim).mockClear();
    await expect(plugin.definition.onEnvironmentAcquireLease!({
      driverKey: "kubernetes",
      companyId: "acme",
      environmentId: "env-1",
      config: CONFIG,
      runId: "run-past",
      requestedExpiresAt: "2020-01-01T00:00:00Z",
    })).rejects.toThrow(/must be in the future/);
    expect(sandboxCrOrchestrator.claim).not.toHaveBeenCalled();
    expect(createPerRunSecret).toHaveBeenCalledTimes(1);
  });

  it("cleans up instead of returning a lease whose lifetime elapsed during acquire", async () => {
    vi.useFakeTimers();
    const initialNow = new Date("2026-10-01T12:00:00.000Z");
    vi.setSystemTime(initialNow);
    vi.mocked(waitForSandboxPodIdentity).mockImplementationOnce(async () => {
      vi.setSystemTime(new Date(initialNow.getTime() + 2_000));
      return {
        name: "sandbox-pod",
        uid: "pod-uid",
        phase: "Pending",
        terminating: false,
      };
    });

    try {
      await expect(plugin.definition.onEnvironmentAcquireLease!({
        driverKey: "kubernetes",
        companyId: "acme",
        environmentId: "env-1",
        config: { ...CONFIG, sandboxLifetimeSec: 1 },
        runId: "run-too-short",
      })).rejects.toThrow(/expired while it was being acquired/);
      expect(destroyLeaseResources).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({
        expectedSandboxUid: "sandbox-uid",
        expectedSecretUid: "secret-uid",
      }));
    } finally {
      vi.useRealTimers();
    }
  });

  it("rejects native sync before touching the Pod when the same name has a new UID", async () => {
    const lease = {
      providerLeaseId: "pc-abc",
      metadata: {
        namespace: "paperclip-acme",
        backend: "sandbox-cr",
        remoteCwd: "/workspace",
        sandboxUid: "sandbox-uid",
        podUid: "pod-uid-original",
      },
    };
    vi.mocked(getSandboxPodIdentity).mockResolvedValueOnce({
      name: "sandbox-pod",
      uid: "pod-uid-replacement",
      phase: "Running",
      terminating: false,
    });

    await expect(plugin.definition.onEnvironmentSyncIn!({
      driverKey: "kubernetes",
      companyId: "acme",
      environmentId: "env-1",
      config: CONFIG,
      lease,
      operations: [{ operationId: "sync-replacement", files: [] }],
    })).rejects.toThrow(/was recreated/);
    expect(execInPodStreaming).not.toHaveBeenCalled();
  });
});
