import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
  clients: {} as Record<string, unknown>,
  identity: vi.fn(),
  exec: vi.fn(async (..._args: unknown[]) => ({ exitCode: 0, stdout: "ok", stderr: "" })),
}));

vi.mock("../../src/kube-client.js", () => ({
  createKubeConfig: vi.fn(() => ({})),
  makeKubeClients: vi.fn(() => h.clients),
}));

vi.mock("@paperclipai/plugin-sdk", () => ({
  definePlugin: (definition: unknown) => ({ definition }),
}));

vi.mock("../../src/sandbox-cr-orchestrator.js", () => ({
  sandboxCrOrchestrator: {
    claim: vi.fn(),
    findPod: vi.fn(),
    waitForCompletion: vi.fn(async () => undefined),
    release: vi.fn(),
  },
  getSandboxPodIdentity: h.identity,
  waitForSandboxPodIdentity: vi.fn(),
  deleteSandboxCrIfUid: vi.fn(),
  SandboxIdentityMismatchError: class SandboxIdentityMismatchError extends Error {},
  SandboxPodIdentityTimeoutError: class SandboxPodIdentityTimeoutError extends Error {},
  SandboxCrTimeoutError: class SandboxCrTimeoutError extends Error {},
}));

vi.mock("../../src/pod-exec.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../src/pod-exec.js")>();
  return { ...actual, execInPod: h.exec };
});

import plugin from "../../src/plugin.js";

const CONFIG = { inCluster: true, backend: "sandbox-cr", podActivityDeadlineSec: 3600 };

function identity(uid = "pod-uid-original") {
  return {
    name: "pc-exec-pod",
    uid,
    phase: "Running",
    terminating: false,
  };
}

function params(env: Record<string, string> = {}) {
  return {
    driverKey: "kubernetes",
    companyId: "acme",
    environmentId: "env-1",
    config: CONFIG,
    lease: {
      providerLeaseId: "pc-exec",
      metadata: {
        namespace: "paperclip-acme",
        backend: "sandbox-cr",
        sandboxUid: "sandbox-uid",
        podUid: "pod-uid-original",
      },
    },
    command: "sh",
    args: ["-c", "printf hello"],
    env,
  };
}

describe("sandbox-cr exec Pod UID guard", () => {
  beforeEach(() => {
    h.clients = {};
    h.identity.mockReset();
    h.identity.mockResolvedValue(identity());
    h.exec.mockClear();
  });

  it("wraps the invocation with the persisted UID and blocks caller env override", async () => {
    const result = await plugin.definition.onEnvironmentExecute!(params({
      PAPERCLIP_SANDBOX_POD_UID: "caller-controlled-value",
      CUSTOM_VALUE: "caller-value",
    }) as never);

    expect(result.exitCode).toBe(0);
    expect(h.identity).toHaveBeenCalledTimes(2);
    const [, , , , command] = h.exec.mock.calls[0]!;
    expect(command[0]).toBe("/bin/sh");
    expect(command[1]).toBe("-c");
    expect(command[2]).toContain('"${PAPERCLIP_SANDBOX_POD_UID:-}" != "$expected_pod_uid"');
    expect(command[2]).toContain("export CUSTOM_VALUE='caller-value';");
    expect(command[2]).not.toContain("caller-controlled-value");
    expect(command.slice(4)).toEqual([
      "pod-uid-original",
      "sh",
      "-c",
      "printf hello",
    ]);
  });

  it("rejects a replacement discovered after the initial API UID check", async () => {
    h.identity
      .mockResolvedValueOnce(identity("pod-uid-original"))
      .mockResolvedValueOnce(identity("pod-uid-replacement"));

    const result = await plugin.definition.onEnvironmentExecute!(params() as never);

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toMatch(/was recreated/);
    expect(h.exec).not.toHaveBeenCalled();
  });
});
