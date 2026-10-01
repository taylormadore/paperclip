import { describe, it, expect } from "vitest";
import {
  buildSandboxCrManifest,
  resolveSandboxShutdownTime,
} from "../../src/sandbox-cr-builder.js";

const baseInput = {
  namespace: "paperclip-acme",
  sandboxName: "pc-01h00000000000000000000000",
  shutdownTime: "2026-01-02T00:00:00.000Z",
  adapterType: "claude_local",
  image: "ghcr.io/paperclipai/agent-runtime-claude:v1",
  envSecretName: "pc-01h00000000000000000000000-env",
  serviceAccountName: "paperclip-tenant-sa",
  labels: { "paperclip.io/run-id": "r1" },
  resources: {
    requests: { cpu: "250m", memory: "512Mi" },
    limits: { cpu: "2", memory: "4Gi" },
  },
  runtimeClassName: undefined,
};

describe("buildSandboxCrManifest", () => {
  it("returns a Sandbox CR with the correct apiVersion and kind", () => {
    const cr = buildSandboxCrManifest(baseInput);
    expect(cr.apiVersion).toBe("agents.x-k8s.io/v1beta1");
    expect(cr.kind).toBe("Sandbox");
  });

  it("sets metadata name and namespace correctly", () => {
    const cr = buildSandboxCrManifest(baseInput);
    expect(cr.metadata.name).toBe(baseInput.sandboxName);
    expect(cr.metadata.namespace).toBe(baseInput.namespace);
  });

  it("sets an absolute shutdown time and controller Delete policy", () => {
    const cr = buildSandboxCrManifest(baseInput);
    expect(cr.spec.shutdownTime).toBe(baseInput.shutdownTime);
    expect(cr.spec.shutdownPolicy).toBe("Delete");
  });

  it("does NOT set ownerReferences (out-of-cluster server, explicit release path)", () => {
    const cr = buildSandboxCrManifest(baseInput);
    expect(cr.metadata.ownerReferences).toBeUndefined();
  });

  it("sets restartPolicy=Always on the pod template (required for long-lived Sandbox pod)", () => {
    const cr = buildSandboxCrManifest(baseInput);
    expect(cr.spec.podTemplate.spec.restartPolicy).toBe("Always");
  });

  it("uses sleep-infinity entrypoint via Tini for multi-command exec", () => {
    const cr = buildSandboxCrManifest(baseInput);
    const container = cr.spec.podTemplate.spec.containers[0];
    expect(container.command).toEqual([
      "/usr/bin/tini",
      "--",
      "/bin/sh",
      "-c",
      "sleep infinity",
    ]);
  });

  it("applies the same security baseline as Job backend (non-root, drop ALL, RO rootFS, seccomp)", () => {
    const cr = buildSandboxCrManifest(baseInput);
    const podSec = cr.spec.podTemplate.spec.securityContext;
    expect(podSec.runAsNonRoot).toBe(true);
    expect(podSec.runAsUser).toBe(1000);
    expect(podSec.fsGroupChangePolicy).toBe("OnRootMismatch");
    expect(podSec.seccompProfile.type).toBe("RuntimeDefault");

    const container = cr.spec.podTemplate.spec.containers[0];
    expect(container.securityContext.runAsNonRoot).toBe(true);
    expect(container.securityContext.readOnlyRootFilesystem).toBe(true);
    expect(container.securityContext.allowPrivilegeEscalation).toBe(false);
    expect(container.securityContext.capabilities.drop).toEqual(["ALL"]);
  });

  it("disables automountServiceAccountToken by default", () => {
    const cr = buildSandboxCrManifest(baseInput);
    expect(cr.spec.podTemplate.spec.automountServiceAccountToken).toBe(false);
  });

  it("mounts the ServiceAccount token only when agentApiAccess is enabled", () => {
    const cr = buildSandboxCrManifest({ ...baseInput, agentApiAccess: true });
    expect(cr.spec.podTemplate.spec.automountServiceAccountToken).toBe(true);
  });

  it("declares emptyDir volume mounts for standard agent paths", () => {
    const cr = buildSandboxCrManifest(baseInput);
    const mounts = cr.spec.podTemplate.spec.containers[0].volumeMounts;
    const mountPaths = mounts
      .map((m: { mountPath: string }) => m.mountPath)
      .sort();
    expect(mountPaths).toEqual([
      "/home/paperclip",
      "/home/paperclip/.cache",
      "/tmp",
      "/workspace",
    ]);

    const volumes = cr.spec.podTemplate.spec.volumes;
    expect(
      volumes.every((v: { emptyDir?: unknown }) => v.emptyDir !== undefined),
    ).toBe(true);
  });

  it("envFrom references the per-run secret", () => {
    const cr = buildSandboxCrManifest(baseInput);
    const envFrom = cr.spec.podTemplate.spec.containers[0].envFrom;
    expect(envFrom[0].secretRef.name).toBe(baseInput.envSecretName);
  });

  it("injects the actual Pod UID through the downward API", () => {
    const cr = buildSandboxCrManifest(baseInput);
    const env = cr.spec.podTemplate.spec.containers[0].env;
    expect(env).toContainEqual({
      name: "PAPERCLIP_SANDBOX_POD_UID",
      valueFrom: { fieldRef: { fieldPath: "metadata.uid" } },
    });
  });

  it("applies runtimeClassName when set", () => {
    const cr = buildSandboxCrManifest({
      ...baseInput,
      runtimeClassName: "kata-fc",
    });
    expect(cr.spec.podTemplate.spec.runtimeClassName).toBe("kata-fc");
  });

  it("does not set runtimeClassName when unset", () => {
    const cr = buildSandboxCrManifest(baseInput);
    expect(cr.spec.podTemplate.spec.runtimeClassName).toBeUndefined();
  });

  it("applies provided labels to CR metadata and pod template labels (with role=agent added)", () => {
    const cr = buildSandboxCrManifest(baseInput);
    expect(cr.metadata.labels["paperclip.io/run-id"]).toBe("r1");
    expect(
      cr.spec.podTemplate.metadata.labels["paperclip.io/run-id"],
    ).toBe("r1");
    expect(cr.spec.podTemplate.metadata.labels["paperclip.io/role"]).toBe(
      "agent",
    );
  });

  it("applies imagePullSecrets when provided", () => {
    const cr = buildSandboxCrManifest({
      ...baseInput,
      imagePullSecrets: ["my-pull-secret"],
    });
    expect(cr.spec.podTemplate.spec.imagePullSecrets).toEqual([
      { name: "my-pull-secret" },
    ]);
  });

  it("does not set imagePullSecrets when not provided", () => {
    const cr = buildSandboxCrManifest(baseInput);
    expect(cr.spec.podTemplate.spec.imagePullSecrets).toBeUndefined();
  });
});

describe("resolveSandboxShutdownTime", () => {
  const now = Date.parse("2026-01-01T00:00:00.000Z");

  it("uses the configured absolute lifetime by default", () => {
    expect(resolveSandboxShutdownTime(86_400, undefined, now)).toBe(
      "2026-01-02T00:00:00.000Z",
    );
  });

  it("honors an earlier requested expiry", () => {
    expect(
      resolveSandboxShutdownTime(86_400, "2026-01-01T01:00:00-04:00", now),
    ).toBe("2026-01-01T05:00:00.000Z");
  });

  it("keeps the provider lifetime when the requested expiry is later", () => {
    expect(
      resolveSandboxShutdownTime(3_600, "2026-01-02T00:00:00Z", now),
    ).toBe("2026-01-01T01:00:00.000Z");
  });

  it("rejects lifetimes outside the ISO date range", () => {
    expect(() => resolveSandboxShutdownTime(Number.MAX_SAFE_INTEGER, undefined, now)).toThrow(
      /invalid shutdownTime/,
    );
  });

  it.each([
    ["malformed", "not-a-timestamp"],
    ["past", "2025-12-31T23:59:59Z"],
  ])("rejects a %s requested expiry", (_label, requested) => {
    expect(() => resolveSandboxShutdownTime(86_400, requested, now)).toThrow(
      /requestedExpiresAt/,
    );
  });
});
