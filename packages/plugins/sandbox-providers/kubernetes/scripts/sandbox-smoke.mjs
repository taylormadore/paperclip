#!/usr/bin/env node

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const READY_TIMEOUT_MS = 240_000;
const CLEANUP_TIMEOUT_MS = 120_000;
const POLL_MS = 1_000;
const CONTAINER = "agent";
const SERVICE_ACCOUNT = "smoke-agent";
const ROLE_NAME = "sandbox-smoke-agent";
const ROLE_BINDING_NAME = "sandbox-smoke-agent";
const TEST_LABEL = "paperclip.io/test-run-id";
const DEFAULT_PAPERCLIP_NAMESPACE = "paperclip";

const HELP = `Usage:
  node packages/plugins/sandbox-providers/kubernetes/scripts/sandbox-smoke.mjs \\
    --plugin-dir <built-plugin-package> \\
    --kubeconfig <path> \\
    --image <runtime-image-reference> \\
    --output <evidence-json-path> \\
    [--agent-api-access]

The script creates a unique restricted namespace, exercises the built
Kubernetes provider's Sandbox create/Ready/pod lookup/exec/delete flow, then
deletes and verifies the namespace in a finally block. --agent-api-access also
creates a narrow namespaced Role and a Cilium kube-apiserver egress policy,
then verifies kubectl ConfigMap CRUD and a Forbidden Secrets read.

The --plugin-dir value is the standalone plugin package directory containing
dist/*.js (or the dist directory itself).`;

function parseArgs(argv) {
  const options = {};
  const valueOptions = new Set(["--plugin-dir", "--kubeconfig", "--image", "--output"]);

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--help" || arg === "-h") {
      return { help: true };
    }
    if (arg === "--agent-api-access") {
      options.agentApiAccess = true;
      continue;
    }
    if (!valueOptions.has(arg)) {
      throw new Error(`Unknown argument: ${arg}`);
    }
    const value = argv[index + 1];
    if (!value || value.startsWith("--")) {
      throw new Error(`Missing value for ${arg}`);
    }
    const optionName = arg.slice(2).replace(/-([a-z])/g, (_match, letter) => letter.toUpperCase());
    options[optionName] = value;
    index += 1;
  }

  for (const name of ["pluginDir", "kubeconfig", "image", "output"]) {
    if (!options[name]) throw new Error(`Missing required option --${name.replace(/[A-Z]/g, (letter) => `-${letter.toLowerCase()}`)}`);
  }
  options.agentApiAccess ??= false;
  return options;
}

function getStatusCode(error) {
  if (!error || typeof error !== "object") return undefined;
  const candidate =
    error.code ??
    error.statusCode ??
    error.response?.statusCode ??
    error.response?.status;
  const numeric = Number(candidate);
  return Number.isFinite(numeric) ? numeric : undefined;
}

function errorMessage(error) {
  if (error instanceof Error) return error.message;
  return String(error);
}

function ownerReferences(object) {
  return Array.isArray(object?.metadata?.ownerReferences)
    ? object.metadata.ownerReferences
    : [];
}

function isOwnedByUid(object, uid) {
  return ownerReferences(object).some((owner) => owner.uid === uid);
}

function isNotFound(error) {
  return getStatusCode(error) === 404;
}

async function sleep(ms) {
  await new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

async function importPluginModules(pluginDir) {
  const packageDir = resolve(pluginDir);
  const distDir = existsSync(join(packageDir, "dist"))
    ? join(packageDir, "dist")
    : packageDir;
  const importDist = async (name) => {
    const modulePath = join(distDir, `${name}.js`);
    if (!existsSync(modulePath)) {
      throw new Error(`Missing built plugin module: ${modulePath}`);
    }
    return import(pathToFileURL(modulePath).href);
  };

  const [kubeClient, sandboxBuilder, sandboxOrchestrator, podExec, sandboxApi, ciliumPolicy] =
    await Promise.all([
      importDist("kube-client"),
      importDist("sandbox-cr-builder"),
      importDist("sandbox-cr-orchestrator"),
      importDist("pod-exec"),
      importDist("sandbox-cr-api"),
      importDist("cilium-network-policy"),
    ]);

  return {
    ...kubeClient,
    ...sandboxBuilder,
    ...sandboxOrchestrator,
    ...podExec,
    ...sandboxApi,
    ...ciliumPolicy,
  };
}

async function waitUntilGone(read, label, checks, timeoutMs = CLEANUP_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      await read();
    } catch (error) {
      if (isNotFound(error)) {
        checks.push(`${label} deleted`);
        return;
      }
      throw error;
    }
    await sleep(POLL_MS);
  }
  throw new Error(`${label} was not deleted within ${timeoutMs}ms`);
}

async function waitForNoOwnedObjects(list, uid, label, checks) {
  const deadline = Date.now() + CLEANUP_TIMEOUT_MS;
  while (Date.now() < deadline) {
    const objects = await list();
    if (!objects.some((object) => isOwnedByUid(object, uid))) {
      checks.push(`owned ${label} deleted`);
      return;
    }
    await sleep(POLL_MS);
  }
  throw new Error(`Sandbox-owned ${label} were not deleted within ${CLEANUP_TIMEOUT_MS}ms`);
}

async function ensureConfigMapRemoved(clients, namespace, name, checks) {
  try {
    await clients.core.deleteNamespacedConfigMap({ name, namespace });
  } catch (error) {
    if (!isNotFound(error)) throw error;
  }
  await waitUntilGone(
    () => clients.core.readNamespacedConfigMap({ name, namespace }),
    `ConfigMap ${name}`,
    checks,
  );
}

function rememberCleanupError(evidence, label, error) {
  evidence.cleanupErrors ??= [];
  evidence.cleanupErrors.push(`${label}: ${errorMessage(error)}`);
  evidence.success = false;
  process.exitCode = 1;
}

async function writeEvidence(evidence, outputPath) {
  const absolutePath = resolve(outputPath);
  mkdirSync(dirname(absolutePath), { recursive: true });
  writeFileSync(absolutePath, `${JSON.stringify(evidence, null, 2)}\n`, "utf8");
  console.log(`Evidence written to ${absolutePath}`);
  console.log(JSON.stringify(evidence, null, 2));
}

async function runSmoke(options) {
  const runId = randomUUID();
  const suffix = runId.replaceAll("-", "").slice(0, 16);
  const namespace = `paperclip-smoke-${suffix}`;
  const sandboxName = "pc-sandbox-smoke";
  const envSecretName = `${sandboxName}-env`;
  const configMapName = `${sandboxName}-api-smoke`;
  const labelValue = runId;
  const evidence = {
    runId,
    namespace,
    sandboxName,
    image: options.image,
    agentApiAccess: options.agentApiAccess,
    startedAt: new Date().toISOString(),
    checks: [],
  };
  const checks = evidence.checks;

  let modules;
  let clients;
  let kubeConfig;
  let namespaceCreateAttempted = false;
  let namespaceCreated = false;
  let sandboxCreated = false;
  let sandboxDeleteRequested = false;
  let sandboxUid;
  let secretCreated = false;
  let configMapCreated = false;
  let configMapDeleted = false;
  let podName;

  const customObjectParams = (name) => ({
    group: modules.SANDBOX_GROUP,
    version: modules.SANDBOX_VERSION,
    namespace,
    plural: modules.SANDBOX_PLURAL,
    name,
  });

  const deleteSandboxAndVerify = async () => {
    if (!sandboxCreated || !modules || !clients) return;
    if (!sandboxDeleteRequested) {
      try {
        await modules.deleteSandboxCr(clients, namespace, sandboxName);
      } catch (error) {
        if (!isNotFound(error)) throw error;
      }
      sandboxDeleteRequested = true;
      checks.push("provider Sandbox delete requested");
    }
    await waitUntilGone(
      () => clients.custom.getNamespacedCustomObject(customObjectParams(sandboxName)),
      "Sandbox CR",
      checks,
    );
    if (sandboxUid) {
      await waitForNoOwnedObjects(
        async () => {
          const response = await clients.core.listNamespacedPod({ namespace });
          return response.items ?? [];
        },
        sandboxUid,
        "Pod",
        checks,
      );
      if (secretCreated) {
        await waitForNoOwnedObjects(
          async () => {
            const response = await clients.core.listNamespacedSecret({ namespace });
            return response.items ?? [];
          },
          sandboxUid,
          "Secret",
          checks,
        );
      }
    }
  };

  try {
    modules = await importPluginModules(options.pluginDir);
    kubeConfig = modules.createKubeConfig({
      kubeconfig: readFileSync(resolve(options.kubeconfig), "utf8"),
    });
    clients = modules.makeKubeClients(kubeConfig);

    const namespaceManifest = {
      apiVersion: "v1",
      kind: "Namespace",
      metadata: {
        name: namespace,
        labels: {
          [TEST_LABEL]: labelValue,
          "pod-security.kubernetes.io/enforce": "restricted",
          "pod-security.kubernetes.io/audit": "restricted",
          "pod-security.kubernetes.io/warn": "restricted",
        },
      },
    };
    namespaceCreateAttempted = true;
    await clients.core.createNamespace({ body: namespaceManifest });
    namespaceCreated = true;
    checks.push("unique restricted namespace created");
    console.log(`Created disposable namespace ${namespace}`);

    await clients.networking.createNamespacedNetworkPolicy({
      namespace,
      body: {
        apiVersion: "networking.k8s.io/v1",
        kind: "NetworkPolicy",
        metadata: { name: "deny-all" },
        spec: { podSelector: {}, policyTypes: ["Ingress", "Egress"] },
      },
    });
    checks.push("default-deny NetworkPolicy created");

    await clients.core.createNamespacedServiceAccount({
      namespace,
      body: {
        apiVersion: "v1",
        kind: "ServiceAccount",
        metadata: { name: SERVICE_ACCOUNT },
        automountServiceAccountToken: false,
      },
    });
    checks.push("tenant ServiceAccount created with token automount disabled by default");

    if (options.agentApiAccess) {
      await clients.rbac.createNamespacedRole({
        namespace,
        body: {
          apiVersion: "rbac.authorization.k8s.io/v1",
          kind: "Role",
          metadata: { name: ROLE_NAME },
          rules: [
            {
              apiGroups: [""],
              resources: ["configmaps"],
              verbs: ["get", "list", "watch", "create", "update", "patch", "delete"],
            },
            { apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "watch"] },
          ],
        },
      });
      await clients.rbac.createNamespacedRoleBinding({
        namespace,
        body: {
          apiVersion: "rbac.authorization.k8s.io/v1",
          kind: "RoleBinding",
          metadata: { name: ROLE_BINDING_NAME },
          roleRef: {
            apiGroup: "rbac.authorization.k8s.io",
            kind: "Role",
            name: ROLE_NAME,
          },
          subjects: [
            { kind: "ServiceAccount", name: SERVICE_ACCOUNT, namespace },
          ],
        },
      });
      checks.push("namespaced Role/RoleBinding grants ConfigMap CRUD and Pod reads only");

      const ciliumManifest = modules.buildCiliumNetworkPolicyManifest({
        namespace,
        paperclipServerNamespace: DEFAULT_PAPERCLIP_NAMESPACE,
        egressAllowFqdns: ["api.openai.com"],
        egressAllowCidrs: [],
        agentApiAccess: true,
      });
      await clients.custom.createNamespacedCustomObject({
        group: "cilium.io",
        version: "v2",
        namespace,
        plural: "ciliumnetworkpolicies",
        body: ciliumManifest,
      });
      const ciliumPolicy = await clients.custom.getNamespacedCustomObject({
        group: "cilium.io",
        version: "v2",
        namespace,
        plural: "ciliumnetworkpolicies",
        name: ciliumManifest.metadata.name,
      });
      const policyEgress = ciliumPolicy?.spec?.egress ?? [];
      assert.ok(
        policyEgress.some((rule) => rule.toEntities?.includes("kube-apiserver")),
        "Cilium egress policy does not allow kube-apiserver",
      );
      checks.push("Cilium policy allows kube-apiserver egress");
    }

    const manifest = modules.buildSandboxCrManifest({
      namespace,
      sandboxName,
      adapterType: "codex_local",
      image: options.image,
      envSecretName,
      serviceAccountName: SERVICE_ACCOUNT,
      agentApiAccess: options.agentApiAccess,
      labels: {
        [TEST_LABEL]: labelValue,
        "app.kubernetes.io/name": "paperclip-sandbox-smoke",
      },
      resources: {
        requests: { cpu: "100m", memory: "128Mi" },
        limits: { cpu: "1", memory: "1Gi" },
      },
    });
    const created = await modules.createSandboxCr(clients, namespace, manifest);
    sandboxUid = created.uid;
    sandboxCreated = true;
    checks.push("provider created Sandbox CR");

    await clients.core.createNamespacedSecret({
      namespace,
      body: {
        apiVersion: "v1",
        kind: "Secret",
        metadata: {
          name: envSecretName,
          ownerReferences: [
            {
              apiVersion: modules.SANDBOX_API_VERSION,
              kind: "Sandbox",
              name: sandboxName,
              uid: sandboxUid,
              controller: true,
              blockOwnerDeletion: true,
            },
          ],
        },
        type: "Opaque",
        stringData: { SMOKE_MARKER: "non-secret" },
      },
    });
    secretCreated = true;
    checks.push("non-secret per-run environment Secret created and owner-linked");

    const status = await modules.waitForSandboxReady(clients, namespace, sandboxName, {
      timeoutMs: READY_TIMEOUT_MS,
      pollMs: 2_000,
    });
    assert.equal(status.phase, "Running");
    checks.push("current-generation Ready condition");

    podName = await modules.findPodForSandbox(clients, namespace, sandboxName);
    assert.ok(podName, "provider could not resolve the Sandbox-owned Pod");
    evidence.podName = podName;
    checks.push("provider resolved controller-owned Pod");
    console.log(`Sandbox Ready; provider selected Pod ${namespace}/${podName}`);

    const identity = await modules.execInPod(
      kubeConfig,
      namespace,
      podName,
      CONTAINER,
      ["/bin/sh", "-c", "id -u; id -g"],
      undefined,
      30_000,
    );
    assert.equal(identity.exitCode, 0, `identity check failed: ${identity.stderr}`);
    assert.deepEqual(
      identity.stdout.trim().split(/\s+/),
      ["1000", "1000"],
      "sandbox process did not run as uid/gid 1000",
    );
    evidence.identity = identity.stdout.trim();
    checks.push("agent process runs as uid/gid 1000");

    const codexVersion = await modules.execInPod(
      kubeConfig,
      namespace,
      podName,
      CONTAINER,
      ["codex", "--version"],
      undefined,
      30_000,
    );
    assert.equal(codexVersion.exitCode, 0, `codex --version failed: ${codexVersion.stderr}`);
    evidence.codexVersion = codexVersion.stdout.trim();
    checks.push("Codex CLI runs");

    const workspaceWrite = await modules.execInPod(
      kubeConfig,
      namespace,
      podName,
      CONTAINER,
      ["/bin/sh", "-c", "printf persisted > /workspace/smoke; printf wrote"],
      undefined,
      30_000,
    );
    assert.equal(workspaceWrite.exitCode, 0, `workspace write failed: ${workspaceWrite.stderr}`);
    const workspaceRead = await modules.execInPod(
      kubeConfig,
      namespace,
      podName,
      CONTAINER,
      ["cat", "/workspace/smoke"],
      undefined,
      30_000,
    );
    assert.equal(workspaceRead.exitCode, 0, `workspace read failed: ${workspaceRead.stderr}`);
    assert.equal(workspaceRead.stdout, "persisted");
    checks.push("multiple provider exec calls and persistent workspace");

    const readOnly = await modules.execInPod(
      kubeConfig,
      namespace,
      podName,
      CONTAINER,
      ["/bin/sh", "-c", "grep \" / \" /proc/mounts | grep -Eq \" ro,\" && echo rootfs-read-only"],
      undefined,
      30_000,
    );
    assert.equal(readOnly.exitCode, 0, `read-only root check failed: ${readOnly.stderr}`);
    assert.match(readOnly.stdout, /rootfs-read-only/);
    checks.push("read-only root filesystem");

    const tokenCheck = await modules.execInPod(
      kubeConfig,
      namespace,
      podName,
      CONTAINER,
      [
        "/bin/sh",
        "-c",
        "if test -s /var/run/secrets/kubernetes.io/serviceaccount/token; then echo token-present; else echo token-absent; fi",
      ],
      undefined,
      30_000,
    );
    assert.equal(tokenCheck.exitCode, 0, `ServiceAccount token check failed: ${tokenCheck.stderr}`);
    const expectedTokenState = options.agentApiAccess ? "token-present" : "token-absent";
    assert.equal(tokenCheck.stdout.trim(), expectedTokenState);
    evidence.serviceAccountToken = expectedTokenState;
    checks.push(options.agentApiAccess ? "opt-in projected tenant token mounted" : "no default agent ServiceAccount token");

    if (options.agentApiAccess) {
      const kubectlVersion = await modules.execInPod(
        kubeConfig,
        namespace,
        podName,
        CONTAINER,
        ["kubectl", "version", "--client", "--output=yaml"],
        undefined,
        30_000,
      );
      assert.equal(kubectlVersion.exitCode, 0, `kubectl version failed: ${kubectlVersion.stderr}`);
      evidence.kubectlVersion = kubectlVersion.stdout.trim();
      checks.push("kubectl client runs");

      const podRead = await modules.execInPod(
        kubeConfig,
        namespace,
        podName,
        CONTAINER,
        ["kubectl", "get", "pod", podName, "--namespace", namespace, "-o", "name"],
        undefined,
        30_000,
      );
      assert.equal(podRead.exitCode, 0, `kubectl get Pod failed: ${podRead.stderr}`);
      assert.equal(podRead.stdout.trim(), `pod/${podName}`);
      evidence.podRead = podRead.stdout.trim();
      checks.push("kubectl read permitted Pod");

      const configMapCreate = await modules.execInPod(
        kubeConfig,
        namespace,
        podName,
        CONTAINER,
        ["kubectl", "create", "configmap", configMapName, "--from-literal=smoke=ok", "--namespace", namespace],
        undefined,
        30_000,
      );
      assert.equal(configMapCreate.exitCode, 0, `kubectl create ConfigMap failed: ${configMapCreate.stderr}`);
      configMapCreated = true;
      evidence.configMapCreate = configMapCreate.stdout.trim();
      checks.push("kubectl created namespaced ConfigMap");

      const configMapRead = await modules.execInPod(
        kubeConfig,
        namespace,
        podName,
        CONTAINER,
        ["kubectl", "get", "configmap", configMapName, "--namespace", namespace, "-o", "jsonpath={.data.smoke}"],
        undefined,
        30_000,
      );
      assert.equal(configMapRead.exitCode, 0, `kubectl get ConfigMap failed: ${configMapRead.stderr}`);
      assert.equal(configMapRead.stdout, "ok");
      evidence.configMapRead = configMapRead.stdout;
      checks.push("kubectl read ConfigMap data");

      const secretsRead = await modules.execInPod(
        kubeConfig,
        namespace,
        podName,
        CONTAINER,
        ["kubectl", "get", "secrets", "--namespace", namespace],
        undefined,
        30_000,
      );
      const secretsDiagnostic = `${secretsRead.stdout}\n${secretsRead.stderr}`;
      assert.notEqual(secretsRead.exitCode, 0, "kubectl get Secrets unexpectedly succeeded");
      assert.match(secretsDiagnostic, /forbidden/i, "Secrets read did not fail with Forbidden");
      evidence.secretsReadDenied = secretsDiagnostic.trim();
      checks.push("kubectl Secrets read rejected with Forbidden");

      const configMapDelete = await modules.execInPod(
        kubeConfig,
        namespace,
        podName,
        CONTAINER,
        ["kubectl", "delete", "configmap", configMapName, "--namespace", namespace],
        undefined,
        30_000,
      );
      assert.equal(configMapDelete.exitCode, 0, `kubectl delete ConfigMap failed: ${configMapDelete.stderr}`);
      configMapDeleted = true;
      evidence.configMapDelete = configMapDelete.stdout.trim();
      await waitUntilGone(
        () => clients.core.readNamespacedConfigMap({ name: configMapName, namespace }),
        `ConfigMap ${configMapName}`,
        checks,
      );
      checks.push("kubectl ConfigMap create/get/delete verified");
    }

    const failedExec = await modules.execInPod(
      kubeConfig,
      namespace,
      podName,
      CONTAINER,
      ["/bin/sh", "-c", "exit 7"],
      undefined,
      30_000,
    );
    assert.equal(failedExec.exitCode, 7, "provider did not propagate a nonzero exec status");
    checks.push("nonzero exec status propagation");

    await deleteSandboxAndVerify();
    evidence.success = true;
  } catch (error) {
    evidence.success = false;
    evidence.error = errorMessage(error);
    process.exitCode = 1;
    console.error(`Sandbox smoke failed: ${evidence.error}`);
    if (clients && namespaceCreated) {
      try {
        const pods = await clients.core.listNamespacedPod({ namespace });
        evidence.podStatus = (pods.items ?? []).map((pod) => ({
          name: pod.metadata?.name,
          phase: pod.status?.phase,
          reason: pod.status?.reason,
          conditions: pod.status?.conditions,
        }));
      } catch {
        // Keep the original failure as primary evidence.
      }
    }
  } finally {
    if (clients && namespaceCreated) {
      if (configMapCreated && !configMapDeleted) {
        try {
          await ensureConfigMapRemoved(clients, namespace, configMapName, checks);
          checks.push("fallback ConfigMap cleanup completed");
        } catch (error) {
          rememberCleanupError(evidence, "ConfigMap cleanup", error);
        }
      }

      if (sandboxCreated) {
        try {
          await deleteSandboxAndVerify();
        } catch (error) {
          rememberCleanupError(evidence, "Sandbox cleanup", error);
        }
      }
    }

    if (clients && namespaceCreateAttempted) {
      let mayDeleteNamespace = namespaceCreated;
      if (!mayDeleteNamespace) {
        try {
          const possiblyCreated = await clients.core.readNamespace({ name: namespace });
          mayDeleteNamespace = possiblyCreated.metadata?.labels?.[TEST_LABEL] === labelValue;
        } catch (error) {
          if (!isNotFound(error)) {
            rememberCleanupError(evidence, "namespace ownership check", error);
          }
        }
      }
      if (mayDeleteNamespace) {
        try {
          await clients.core.deleteNamespace({ name: namespace });
          await waitUntilGone(
            () => clients.core.readNamespace({ name: namespace }),
            "disposable namespace",
            checks,
          );
        } catch (error) {
          if (!isNotFound(error)) rememberCleanupError(evidence, "namespace cleanup", error);
        }
      }
    }

    evidence.finishedAt = new Date().toISOString();
    try {
      await writeEvidence(evidence, options.output);
    } catch (error) {
      process.exitCode = 1;
      console.error(`Could not write smoke evidence: ${errorMessage(error)}`);
    }
  }
}

async function main() {
  let options;
  try {
    options = parseArgs(process.argv.slice(2));
  } catch (error) {
    console.error(`${errorMessage(error)}\n\n${HELP}`);
    process.exitCode = 2;
    return;
  }
  if (options.help) {
    console.log(HELP);
    return;
  }
  await runSmoke(options);
}

main().catch((error) => {
  console.error(errorMessage(error));
  process.exitCode = 1;
});
