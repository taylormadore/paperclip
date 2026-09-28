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

vi.mock("../../src/secret-manager.js", () => ({
  createPerRunSecret: vi.fn(async () => undefined),
}));

vi.mock("../../src/sandbox-cr-orchestrator.js", () => ({
  sandboxCrOrchestrator: {
    claim: vi.fn(async () => ({ uid: "sandbox-uid" })),
    findPod: vi.fn(async () => "sandbox-pod"),
    waitForCompletion: vi.fn(async () => undefined),
    release: vi.fn(async () => undefined),
  },
  SandboxCrTimeoutError: class SandboxCrTimeoutError extends Error {},
}));

vi.mock("../../src/pod-exec.js", () => ({
  execInPod: vi.fn(),
  execInPodStreaming: vi.fn(async () => ({ exitCode: 0, stderr: "" })),
  wrapCommandWithEnv: vi.fn(),
}));

import plugin from "../../src/plugin.js";
import { execInPodStreaming } from "../../src/pod-exec.js";

const CONFIG = {
  inCluster: true,
  backend: "sandbox-cr",
  runtimeImage: "ghcr.io/paperclipai/agent:test",
};

beforeEach(() => {
  h.clients = {};
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
});
