import { describe, it, expect, vi } from "vitest";
import {
  createSandboxCr,
  deleteSandboxCr,
  getSandboxCrStatus,
  findPodForSandbox,
  getSandboxPodIdentity,
  waitForSandboxPodIdentity,
  SandboxIdentityMismatchError,
  SandboxCrTimeoutError,
  waitForSandboxReady,
} from "../../src/sandbox-cr-orchestrator.js";

const SANDBOX_GROUP = "agents.x-k8s.io";
const SANDBOX_VERSION = "v1beta1";
const SANDBOX_API_VERSION = `${SANDBOX_GROUP}/${SANDBOX_VERSION}`;
const SANDBOX_PLURAL = "sandboxes";
const SANDBOX_NAME = "pc-abc";
const SANDBOX_UID = "sandbox-uid-123";
const GENERATION = 4;

type Condition = Record<string, unknown>;

function readyCondition(
  status: "True" | "False" | "Unknown" = "True",
  extras: Record<string, unknown> = {},
): Condition {
  return {
    type: "Ready",
    status,
    reason: status === "True" ? "DependenciesReady" : "DependenciesNotReady",
    observedGeneration: GENERATION,
    ...extras,
  };
}

function makeCr(input: {
  conditions?: Condition[];
  generation?: number;
  uid?: string;
  selector?: string;
  annotations?: Record<string, string>;
  deletionTimestamp?: string;
} = {}): Record<string, unknown> {
  return {
    metadata: {
      uid: input.uid ?? SANDBOX_UID,
      generation: input.generation ?? GENERATION,
      ...(input.annotations ? { annotations: input.annotations } : {}),
      ...(input.deletionTimestamp
        ? { deletionTimestamp: input.deletionTimestamp }
        : {}),
    },
    status: {
      ...(input.conditions ? { conditions: input.conditions } : {}),
      ...(input.selector ? { selector: input.selector } : {}),
    },
  };
}

function ownedPod(
  name: string,
  options: {
    uid?: string;
    podUid?: string;
    controller?: boolean;
    phase?: string;
    deletionTimestamp?: string;
  } = {},
): Record<string, unknown> {
  return {
    metadata: {
      name,
      uid: options.podUid ?? "pod-uid-123",
      labels: { "agents.x-k8s.io/sandbox-name-hash": "1a2b3c" },
      ...(options.deletionTimestamp ? { deletionTimestamp: options.deletionTimestamp } : {}),
      ownerReferences: [
        {
          apiVersion: SANDBOX_API_VERSION,
          kind: "Sandbox",
          name: SANDBOX_NAME,
          uid: options.uid ?? SANDBOX_UID,
          controller: options.controller ?? true,
        },
      ],
    },
    status: { phase: options.phase ?? "Running" },
  };
}

describe("createSandboxCr", () => {
  it("uses the v1beta1 API and returns the created UID", async () => {
    const create = vi.fn().mockResolvedValue({ metadata: { uid: "test-uid" } });
    const clients = { custom: { createNamespacedCustomObject: create } };
    const manifest = {
      apiVersion: SANDBOX_API_VERSION,
      kind: "Sandbox",
      metadata: { name: SANDBOX_NAME, namespace: "paperclip-acme" },
    };

    const result = await createSandboxCr(clients as never, "paperclip-acme", manifest);

    expect(create).toHaveBeenCalledWith({
      group: SANDBOX_GROUP,
      version: SANDBOX_VERSION,
      namespace: "paperclip-acme",
      plural: SANDBOX_PLURAL,
      body: manifest,
    });
    expect(result.uid).toBe("test-uid");
  });

  it("throws if the API response has no UID", async () => {
    const create = vi.fn().mockResolvedValue({ metadata: {} });
    const clients = { custom: { createNamespacedCustomObject: create } };

    await expect(createSandboxCr(clients as never, "ns", {})).rejects.toThrow(
      "Sandbox CR created without a UID",
    );
  });
});

describe("getSandboxCrStatus", () => {
  it("maps a current v1beta1 Ready condition to an active sandbox", async () => {
    const get = vi.fn().mockResolvedValue(makeCr({ conditions: [readyCondition()] }));
    const clients = { custom: { getNamespacedCustomObject: get } };

    const status = await getSandboxCrStatus(clients as never, "ns", SANDBOX_NAME);

    expect(status).toMatchObject({ phase: "Running", active: 1, complete: false });
    expect(get).toHaveBeenCalledWith({
      group: SANDBOX_GROUP,
      version: SANDBOX_VERSION,
      namespace: "ns",
      plural: SANDBOX_PLURAL,
      name: SANDBOX_NAME,
    });
  });

  it("does not treat a stale Ready condition as active", async () => {
    const get = vi.fn().mockResolvedValue(
      makeCr({
        conditions: [readyCondition("True", { observedGeneration: GENERATION - 1 })],
      }),
    );
    const clients = { custom: { getNamespacedCustomObject: get } };

    const status = await getSandboxCrStatus(clients as never, "ns", SANDBOX_NAME);

    expect(status.phase).toBe("Pending");
    expect(status.active).toBe(0);
  });

  it("maps Finished/PodFailed to terminal failure status", async () => {
    const get = vi.fn().mockResolvedValue(
      makeCr({
        conditions: [
          {
            type: "Finished",
            status: "True",
            reason: "PodFailed",
            message: "container exited",
            observedGeneration: GENERATION,
          },
        ],
      }),
    );
    const clients = { custom: { getNamespacedCustomObject: get } };

    const status = await getSandboxCrStatus(clients as never, "ns", SANDBOX_NAME);

    expect(status).toMatchObject({
      phase: "Failed",
      complete: true,
      failed: 1,
      reason: "PodFailed",
    });
  });

  it("ignores a stale Finished condition", async () => {
    const get = vi.fn().mockResolvedValue(
      makeCr({
        conditions: [
          {
            type: "Finished",
            status: "True",
            reason: "PodFailed",
            observedGeneration: GENERATION - 1,
          },
        ],
      }),
    );
    const clients = { custom: { getNamespacedCustomObject: get } };

    const status = await getSandboxCrStatus(clients as never, "ns", SANDBOX_NAME);

    expect(status.phase).toBe("Pending");
    expect(status.complete).toBe(false);
  });

  it("maps deletionTimestamp to Terminating", async () => {
    const get = vi.fn().mockResolvedValue(
      makeCr({ deletionTimestamp: "2026-09-26T00:00:00Z" }),
    );
    const clients = { custom: { getNamespacedCustomObject: get } };

    const status = await getSandboxCrStatus(clients as never, "ns", SANDBOX_NAME);

    expect(status.phase).toBe("Running");
    expect(status.reason).toBe("Terminating");
  });
});

describe("findPodForSandbox", () => {
  it("uses the legacy warm-pool Pod annotation and verifies controller ownership", async () => {
    const get = vi.fn().mockResolvedValue(
      makeCr({ annotations: { "agents.x-k8s.io/pod-name": "pc-abc-warm" } }),
    );
    const read = vi.fn().mockResolvedValue(ownedPod("pc-abc-warm"));
    const list = vi.fn();
    const clients = {
      custom: { getNamespacedCustomObject: get },
      core: { readNamespacedPod: read, listNamespacedPod: list },
    };

    const result = await findPodForSandbox(clients as never, "ns", SANDBOX_NAME);

    expect(result).toBe("pc-abc-warm");
    expect(read).toHaveBeenCalledWith({ namespace: "ns", name: "pc-abc-warm" });
    expect(list).not.toHaveBeenCalled();
  });

  it("uses the v1beta1 status.selector when the exact-name Pod is absent", async () => {
    const selector = "agents.x-k8s.io/sandbox-name-hash=1a2b3c";
    const get = vi.fn().mockResolvedValue(makeCr({ selector }));
    const read = vi.fn().mockRejectedValue({ code: 404 });
    const list = vi.fn().mockResolvedValue({ items: [ownedPod(SANDBOX_NAME)] });
    const clients = {
      custom: { getNamespacedCustomObject: get },
      core: { readNamespacedPod: read, listNamespacedPod: list },
    };

    const result = await findPodForSandbox(clients as never, "ns", SANDBOX_NAME);

    expect(result).toBe(SANDBOX_NAME);
    expect(list).toHaveBeenCalledWith({ namespace: "ns", labelSelector: selector });
  });

  it("rejects a matching owner UID that is not the controller reference", async () => {
    const selector = "agents.x-k8s.io/sandbox-name-hash=1a2b3c";
    const get = vi.fn().mockResolvedValue(makeCr({ selector }));
    const read = vi.fn().mockRejectedValue({ code: 404 });
    const list = vi.fn().mockResolvedValue({
      items: [ownedPod("pc-abc-impostor", { controller: false })],
    });
    const clients = {
      custom: { getNamespacedCustomObject: get },
      core: { readNamespacedPod: read, listNamespacedPod: list },
    };

    await expect(findPodForSandbox(clients as never, "ns", SANDBOX_NAME)).resolves.toBeNull();
  });

  it("fails closed if the Sandbox UID is missing", async () => {
    const get = vi.fn().mockResolvedValue(makeCr({ uid: "" }));
    const read = vi.fn();
    const list = vi.fn();
    const clients = {
      custom: { getNamespacedCustomObject: get },
      core: { readNamespacedPod: read, listNamespacedPod: list },
    };

    await expect(findPodForSandbox(clients as never, "ns", SANDBOX_NAME)).resolves.toBeNull();
    expect(read).not.toHaveBeenCalled();
    expect(list).not.toHaveBeenCalled();
  });

  it("rejects a same-name replacement Sandbox against the persisted UID", async () => {
    const clients = {
      custom: {
        getNamespacedCustomObject: vi.fn().mockResolvedValue(makeCr({ uid: "replacement-uid" })),
      },
      core: { readNamespacedPod: vi.fn() },
    };
    await expect(
      findPodForSandbox(clients as never, "ns", SANDBOX_NAME, SANDBOX_UID),
    ).rejects.toBeInstanceOf(SandboxIdentityMismatchError);
  });

  it("propagates non-404 errors from exact Pod lookup", async () => {
    const get = vi.fn().mockResolvedValue(makeCr());
    const read = vi.fn().mockRejectedValue({ code: 403, message: "forbidden" });
    const clients = {
      custom: { getNamespacedCustomObject: get },
      core: { readNamespacedPod: read, listNamespacedPod: vi.fn() },
    };

    await expect(findPodForSandbox(clients as never, "ns", SANDBOX_NAME)).rejects.toMatchObject({
      code: 403,
    });
  });
});

describe("waitForSandboxPodIdentity", () => {
  it("returns an owned Pending Pod identity without waiting for Ready", async () => {
    const clients = {
      custom: { getNamespacedCustomObject: vi.fn().mockResolvedValue(makeCr()) },
      core: {
        readNamespacedPod: vi.fn().mockResolvedValue(
          ownedPod(SANDBOX_NAME, { phase: "Pending", podUid: "pod-pending-uid" }),
        ),
      },
    };

    await expect(
      waitForSandboxPodIdentity(clients as never, "ns", SANDBOX_NAME, SANDBOX_UID, {
        timeoutMs: 100,
        pollMs: 10,
      }),
    ).resolves.toEqual({
      name: SANDBOX_NAME,
      uid: "pod-pending-uid",
      phase: "Pending",
      terminating: false,
    });
  });

  it("times out while waiting for an owned Pod UID without a real ten-second wait", async () => {
    vi.useFakeTimers();
    try {
      const clients = {
        custom: { getNamespacedCustomObject: vi.fn().mockResolvedValue(makeCr()) },
        core: {
          readNamespacedPod: vi.fn().mockRejectedValue(Object.assign(new Error("gone"), { code: 404 })),
        },
      };
      const waiting = waitForSandboxPodIdentity(
        clients as never,
        "ns",
        SANDBOX_NAME,
        SANDBOX_UID,
        { timeoutMs: 100, pollMs: 50 },
      );
      const rejected = expect(waiting).rejects.toThrow(
        /did not produce an owned Pod with a UID within 100ms/,
      );
      await vi.advanceTimersByTimeAsync(100);
      await rejected;
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not capture a terminating Pod as a new lease identity", async () => {
    vi.useFakeTimers();
    try {
      const clients = {
        custom: { getNamespacedCustomObject: vi.fn().mockResolvedValue(makeCr()) },
        core: {
          readNamespacedPod: vi.fn().mockResolvedValue(ownedPod(SANDBOX_NAME, {
            deletionTimestamp: "2026-10-01T00:00:00Z",
          })),
        },
      };
      const waiting = waitForSandboxPodIdentity(
        clients as never,
        "ns",
        SANDBOX_NAME,
        SANDBOX_UID,
        { timeoutMs: 100, pollMs: 50 },
      );
      const rejected = expect(waiting).rejects.toThrow(/did not produce an owned Pod with a UID/);
      await vi.advanceTimersByTimeAsync(100);
      await rejected;
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("deleteSandboxCr", () => {
  it("uses the v1beta1 API and Foreground propagation", async () => {
    const del = vi.fn().mockResolvedValue({});
    const clients = { custom: { deleteNamespacedCustomObject: del } };

    await deleteSandboxCr(clients as never, "ns", SANDBOX_NAME);

    expect(del).toHaveBeenCalledWith(
      expect.objectContaining({
        group: SANDBOX_GROUP,
        version: SANDBOX_VERSION,
        namespace: "ns",
        plural: SANDBOX_PLURAL,
        name: SANDBOX_NAME,
        propagationPolicy: "Foreground",
      }),
    );
  });
});

describe("waitForSandboxReady", () => {
  it("resolves for a current Ready condition", async () => {
    const get = vi.fn().mockResolvedValue(makeCr({ conditions: [readyCondition()] }));
    const clients = { custom: { getNamespacedCustomObject: get } };

    const status = await waitForSandboxReady(clients as never, "ns", SANDBOX_NAME, {
      timeoutMs: 500,
      pollMs: 1,
    });

    expect(status.phase).toBe("Running");
    expect(get).toHaveBeenCalledTimes(1);
  });

  it("polls past stale Ready until observedGeneration catches up", async () => {
    const get = vi
      .fn()
      .mockResolvedValueOnce(
        makeCr({
          conditions: [readyCondition("True", { observedGeneration: GENERATION - 1 })],
        }),
      )
      .mockResolvedValueOnce(makeCr({ conditions: [readyCondition()] }));
    const clients = { custom: { getNamespacedCustomObject: get } };

    const status = await waitForSandboxReady(clients as never, "ns", SANDBOX_NAME, {
      timeoutMs: 500,
      pollMs: 1,
    });

    expect(status.phase).toBe("Running");
    expect(get).toHaveBeenCalledTimes(2);
  });

  it("retries a transient ReconcilerError and resolves when the controller recovers", async () => {
    const get = vi
      .fn()
      .mockResolvedValueOnce(
        makeCr({
          conditions: [
            readyCondition("False", {
              reason: "ReconcilerError",
              message: "temporary API error",
            }),
          ],
        }),
      )
      .mockResolvedValueOnce(makeCr({ conditions: [readyCondition()] }));
    const clients = { custom: { getNamespacedCustomObject: get } };

    const status = await waitForSandboxReady(clients as never, "ns", SANDBOX_NAME, {
      timeoutMs: 500,
      pollMs: 1,
    });

    expect(status.phase).toBe("Running");
    expect(get).toHaveBeenCalledTimes(2);
  });

  it("ignores a stale terminal Ready=False condition", async () => {
    const get = vi
      .fn()
      .mockResolvedValueOnce(
        makeCr({
          conditions: [
            readyCondition("False", {
              reason: "PodFailed",
              observedGeneration: GENERATION - 1,
            }),
          ],
        }),
      )
      .mockResolvedValueOnce(makeCr({ conditions: [readyCondition()] }));
    const clients = { custom: { getNamespacedCustomObject: get } };

    const status = await waitForSandboxReady(clients as never, "ns", SANDBOX_NAME, {
      timeoutMs: 500,
      pollMs: 1,
    });

    expect(status.phase).toBe("Running");
    expect(get).toHaveBeenCalledTimes(2);
  });

  it("throws for current terminal PodFailed readiness", async () => {
    const get = vi.fn().mockResolvedValue(
      makeCr({
        conditions: [
          readyCondition("False", {
            reason: "PodFailed",
            message: "container exited",
          }),
        ],
      }),
    );
    const clients = { custom: { getNamespacedCustomObject: get } };

    await expect(
      waitForSandboxReady(clients as never, "ns", SANDBOX_NAME, {
        timeoutMs: 500,
        pollMs: 1,
      }),
    ).rejects.toThrow(/failed.*PodFailed/i);
  });

  it("throws SandboxCrTimeoutError when Ready never becomes current", async () => {
    const get = vi.fn().mockResolvedValue(
      makeCr({
        conditions: [readyCondition("True", { observedGeneration: GENERATION - 1 })],
      }),
    );
    const clients = { custom: { getNamespacedCustomObject: get } };

    await expect(
      waitForSandboxReady(clients as never, "ns", SANDBOX_NAME, {
        timeoutMs: 20,
        pollMs: 1,
      }),
    ).rejects.toBeInstanceOf(SandboxCrTimeoutError);
  });
});
