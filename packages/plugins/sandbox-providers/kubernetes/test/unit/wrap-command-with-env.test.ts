import { describe, expect, it } from "vitest";
import { wrapCommandWithEnv, wrapCommandWithPodUid } from "../../src/pod-exec.js";

describe("wrapCommandWithEnv", () => {
  it("returns the command unchanged when there is no env", () => {
    expect(wrapCommandWithEnv(["opencode", "run"], undefined)).toEqual(["opencode", "run"]);
    expect(wrapCommandWithEnv(["opencode", "run"], {})).toEqual(["opencode", "run"]);
  });

  it("exports the env vars and execs the original command", () => {
    const out = wrapCommandWithEnv(
      ["opencode", "run", "--model", "anthropic/x"],
      { XDG_CONFIG_HOME: "/tmp/cfg", ANTHROPIC_API_KEY: "sk-bf-1" },
    );
    expect(out[0]).toBe("/bin/sh");
    expect(out[1]).toBe("-c");
    expect(out[2]).toContain("export XDG_CONFIG_HOME='/tmp/cfg';");
    expect(out[2]).toContain("export ANTHROPIC_API_KEY='sk-bf-1';");
    expect(out[2]).toContain("exec 'opencode' 'run' '--model' 'anthropic/x'");
  });

  it("never propagates PATH (would break command resolution in the sandbox image)", () => {
    const out = wrapCommandWithEnv(["opencode"], { PATH: "/server/bin", XDG_CONFIG_HOME: "/c" });
    expect(out[2]).not.toContain("PATH=");
    expect(out[2]).toContain("XDG_CONFIG_HOME=");
  });

  it("skips invalid identifiers and non-string values", () => {
    const out = wrapCommandWithEnv(["opencode"], {
      "BAD-KEY": "x",
      GOOD_KEY: "y",
      // @ts-expect-error intentional non-string to exercise the guard
      NUMERIC: 5,
    });
    expect(out[2]).toContain("export GOOD_KEY='y';");
    expect(out[2]).not.toContain("BAD-KEY");
    expect(out[2]).not.toContain("NUMERIC");
  });

  it("shell-escapes single quotes in values", () => {
    const out = wrapCommandWithEnv(["opencode"], { V: "a'b" });
    expect(out[2]).toContain("export V='a'\\''b';");
  });
});

describe("wrapCommandWithPodUid", () => {
  it("guards with positional UID and command args before applying caller env", () => {
    const expectedUid = "pod-uid' ; echo injected";
    const command = ["/usr/bin/agent", "argument with spaces", "x; echo command-injected"];
    const wrapped = wrapCommandWithPodUid(
      command,
      {
        PAPERCLIP_SANDBOX_POD_UID: "caller-must-not-override",
        CUSTOM_VALUE: "caller value with ' quote",
      },
      expectedUid,
    );
    const script = wrapped[2]!;

    expect(wrapped.slice(0, 2)).toEqual(["/bin/sh", "-c"]);
    expect(wrapped.slice(4)).toEqual([expectedUid, ...command]);
    expect(script.indexOf("if [")).toBeLessThan(script.indexOf("export CUSTOM_VALUE="));
    expect(script).toContain('exec "$@"');
    expect(script).not.toContain("caller-must-not-override");
    expect(script).not.toContain(expectedUid);
    expect(script).not.toContain("command-injected");
  });
});
