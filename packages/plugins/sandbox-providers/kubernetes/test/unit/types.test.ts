import { describe, it, expect } from "vitest";
import { kubernetesProviderConfigSchema, parseKubernetesProviderConfig } from "../../src/types.js";

describe("kubernetesProviderConfigSchema", () => {
  it("accepts inCluster=true with no kubeconfig", () => {
    const parsed = parseKubernetesProviderConfig({ inCluster: true });
    expect(parsed.inCluster).toBe(true);
    expect(parsed.namespacePrefix).toBe("paperclip-");
    expect(parsed.imageAllowList).toEqual([]);
    expect(parsed.egressMode).toBe("standard");
    expect(parsed.agentApiAccess).toBe(false);
    expect(parsed.jobTtlSecondsAfterFinished).toBe(900);
  });

  it("accepts sandbox-cr API access only with Cilium egress", () => {
    const parsed = parseKubernetesProviderConfig({
      inCluster: true,
      backend: "sandbox-cr",
      egressMode: "cilium",
      agentApiAccess: true,
    });
    expect(parsed.agentApiAccess).toBe(true);
  });

  it("rejects agentApiAccess with the job backend or standard egress", () => {
    expect(() =>
      parseKubernetesProviderConfig({
        inCluster: true,
        backend: "job",
        egressMode: "cilium",
        agentApiAccess: true,
      }),
    ).toThrow(/agentApiAccess requires backend `sandbox-cr` and egressMode `cilium`/);

    expect(() =>
      parseKubernetesProviderConfig({
        inCluster: true,
        backend: "sandbox-cr",
        egressMode: "standard",
        agentApiAccess: true,
      }),
    ).toThrow(/agentApiAccess requires backend `sandbox-cr` and egressMode `cilium`/);
  });

  it("accepts an exact environment runtimeImage", () => {
    const parsed = parseKubernetesProviderConfig({
      inCluster: true,
      runtimeImage: "registry.example/agent@sha256:abc123",
    });
    expect(parsed.runtimeImage).toBe("registry.example/agent@sha256:abc123");
  });

  it("accepts inline kubeconfig", () => {
    const parsed = parseKubernetesProviderConfig({
      inCluster: false,
      kubeconfig: "apiVersion: v1\nkind: Config\n",
    });
    expect(parsed.kubeconfig).toContain("apiVersion");
  });

  it("rejects when neither inCluster nor any kubeconfig source is set", () => {
    expect(() => parseKubernetesProviderConfig({ inCluster: false })).toThrow(
      /requires one of `inCluster` or `kubeconfig`/,
    );
  });

  it("rejects invalid companySlug", () => {
    expect(() =>
      parseKubernetesProviderConfig({ inCluster: true, companySlug: "INVALID UPPER" }),
    ).toThrow();
  });

  it("rejects egressAllowCidrs entries that are not valid CIDR", () => {
    expect(() =>
      parseKubernetesProviderConfig({ inCluster: true, egressAllowCidrs: ["not-a-cidr"] }),
    ).toThrow(/CIDR/i);
  });
});
