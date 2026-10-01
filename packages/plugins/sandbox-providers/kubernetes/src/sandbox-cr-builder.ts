/**
 * Builds a kubernetes-sigs/agent-sandbox Sandbox CR manifest.
 *
 * The Sandbox CR creates a long-lived pod (sleep infinity entrypoint) into
 * which paperclip-server can exec arbitrary commands. This solves the
 * architectural mismatch with the batch/v1 Job backend, which only supports
 * a single one-shot entrypoint — not the multi-command adapter-install pattern
 * used by paperclip-server.
 *
 * Security baseline is identical to buildJobManifest (pod-spec-builder.ts):
 * non-root, drop ALL caps, read-only rootFS, Tini PID 1, seccomp
 * RuntimeDefault, fsGroupChangePolicy OnRootMismatch, and no ServiceAccount
 * token unless the environment administrator opts into agentApiAccess.
 *
 * NOTE: paperclip-server runs OUTSIDE the cluster, so we cannot set ownerReferences
 * on the Sandbox CR (the owner would need to be an in-cluster resource). The
 * release path is explicit delete via sandboxCrOrchestrator.release().
 */

import { SANDBOX_API_VERSION } from "./sandbox-cr-api.js";

function parseExplicitExpiry(value: string): number {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (!match) {
    throw new Error("requestedExpiresAt must be a valid ISO 8601 timestamp with a timezone");
  }
  const [, yearText, monthText, dayText, hourText, minuteText, secondText, zone] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  const zoneOffset = zone === "Z" ? null : /^([+-])(\d{2}):(\d{2})$/.exec(zone);
  if (
    year === 0 || month < 1 || month > 12 || day < 1 || day > daysInMonth[month - 1] ||
    hour > 23 || minute > 59 || second > 59 ||
    (zoneOffset && (Number(zoneOffset[2]) > 23 || Number(zoneOffset[3]) > 59))
  ) {
    throw new Error("requestedExpiresAt must be a valid ISO 8601 timestamp with a timezone");
  }
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) {
    throw new Error("requestedExpiresAt must be a valid ISO 8601 timestamp with a timezone");
  }
  return timestamp;
}

/** Resolve the CR's absolute shutdown deadline without ever extending a caller's deadline. */
export function resolveSandboxShutdownTime(
  lifetimeSeconds: number,
  requestedExpiresAt?: string | null,
  nowMs = Date.now(),
): string {
  if (!Number.isSafeInteger(lifetimeSeconds) || lifetimeSeconds <= 0) {
    throw new Error("sandboxLifetimeSec must be a positive integer");
  }
  const providerDeadline = nowMs + lifetimeSeconds * 1000;
  if (!Number.isFinite(new Date(providerDeadline).getTime())) {
    throw new Error("sandboxLifetimeSec produces an invalid shutdownTime");
  }
  if (requestedExpiresAt === undefined || requestedExpiresAt === null) {
    return new Date(providerDeadline).toISOString();
  }
  const requestedDeadline = parseExplicitExpiry(requestedExpiresAt);
  if (requestedDeadline <= nowMs) {
    throw new Error("requestedExpiresAt must be in the future");
  }
  return new Date(Math.min(providerDeadline, requestedDeadline)).toISOString();
}

export interface BuildSandboxCrManifestInput {
  namespace: string;
  sandboxName: string;
  shutdownTime: string;
  adapterType: string;
  image: string;
  envSecretName: string;
  serviceAccountName: string;
  /** Mount the tenant ServiceAccount's projected token into the agent pod. */
  agentApiAccess?: boolean;
  labels: Record<string, string>;
  resources: {
    requests?: { cpu?: string; memory?: string };
    limits?: { cpu?: string; memory?: string };
  };
  runtimeClassName?: string;
  imagePullSecrets?: string[];
}

export function buildSandboxCrManifest(
  input: BuildSandboxCrManifestInput,
): Record<string, unknown> {
  const podLabels: Record<string, string> = {
    ...input.labels,
    "paperclip.io/role": "agent",
  };
  return {
    apiVersion: SANDBOX_API_VERSION,
    kind: "Sandbox",
    metadata: {
      name: input.sandboxName,
      namespace: input.namespace,
      labels: { ...input.labels },
      // No ownerReferences: paperclip-server is out-of-cluster. Release is
      // explicit delete.
    },
    spec: {
      shutdownTime: input.shutdownTime,
      shutdownPolicy: "Delete",
      podTemplate: {
        metadata: {
          labels: podLabels,
        },
        spec: {
          serviceAccountName: input.serviceAccountName,
          // Token mounting is disabled by default and enabled only when the
          // provider config opts into the tenant ServiceAccount's scoped API access.
          automountServiceAccountToken: input.agentApiAccess ?? false,
          // Sandbox controller requires restartPolicy: Always so the pod
          // stays running between exec calls.
          restartPolicy: "Always",
          ...(input.runtimeClassName
            ? { runtimeClassName: input.runtimeClassName }
            : {}),
          ...(input.imagePullSecrets && input.imagePullSecrets.length > 0
            ? {
                imagePullSecrets: input.imagePullSecrets.map((name) => ({
                  name,
                })),
              }
            : {}),
          securityContext: {
            runAsNonRoot: true,
            runAsUser: 1000,
            runAsGroup: 1000,
            fsGroup: 1000,
            fsGroupChangePolicy: "OnRootMismatch",
            seccompProfile: { type: "RuntimeDefault" },
          },
          containers: [
            {
              name: "agent",
              image: input.image,
              imagePullPolicy: "IfNotPresent",
              // sleep infinity keeps the pod running; paperclip-server execs
              // commands into it via Kubernetes exec API. Tini as PID 1 for
              // proper signal forwarding and zombie reaping.
              command: [
                "/usr/bin/tini",
                "--",
                "/bin/sh",
                "-c",
                "sleep infinity",
              ],
              // HOME must point at a writable mount; the image's default
               // HOME=/home/node is inside the readOnly root filesystem.
               // Claude (and most agent runtimes) silently exit with code 0
               // and no output when HOME is unwritable, so set this explicitly.
              env: [
                { name: "HOME", value: "/home/paperclip" },
                {
                  name: "PAPERCLIP_SANDBOX_POD_UID",
                  valueFrom: { fieldRef: { fieldPath: "metadata.uid" } },
                },
              ],
              envFrom: [{ secretRef: { name: input.envSecretName } }],
              securityContext: {
                runAsNonRoot: true,
                runAsUser: 1000,
                runAsGroup: 1000,
                readOnlyRootFilesystem: true,
                allowPrivilegeEscalation: false,
                capabilities: { drop: ["ALL"] },
              },
              resources: {
                requests: input.resources.requests ?? {
                  cpu: "250m",
                  memory: "512Mi",
                },
                limits: input.resources.limits ?? {
                  cpu: "2",
                  memory: "4Gi",
                },
              },
              volumeMounts: [
                { name: "workspace", mountPath: "/workspace" },
                { name: "home", mountPath: "/home/paperclip" },
                { name: "cache", mountPath: "/home/paperclip/.cache" },
                { name: "tmp", mountPath: "/tmp" },
              ],
            },
          ],
          volumes: [
            { name: "workspace", emptyDir: { sizeLimit: "8Gi" } },
            { name: "home", emptyDir: { sizeLimit: "1Gi" } },
            { name: "cache", emptyDir: { sizeLimit: "1Gi" } },
            { name: "tmp", emptyDir: { sizeLimit: "2Gi" } },
          ],
        },
      },
    },
  };
}
