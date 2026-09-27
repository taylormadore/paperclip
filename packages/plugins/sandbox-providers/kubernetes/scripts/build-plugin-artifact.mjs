#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import {
  cpSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  rmSync,
  readFileSync,
  realpathSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const packageDir = path.resolve(scriptDir, "..");
const repositoryRoot = path.resolve(packageDir, "../../../..");
const packageJsonPath = path.join(packageDir, "package.json");
const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf8"));

function parseOutputDirectory(argv) {
  const optionIndex = argv.indexOf("--out");
  if (optionIndex < 0 || !argv[optionIndex + 1]) {
    throw new Error("Usage: node scripts/build-plugin-artifact.mjs --out <new-directory>");
  }
  return path.resolve(argv[optionIndex + 1]);
}

function assertExactRuntimeVersions(dependencies) {
  for (const [name, version] of Object.entries(dependencies)) {
    if (typeof version !== "string" || !/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(version)) {
      throw new Error(`Runtime dependency ${name} must use an exact version, got ${JSON.stringify(version)}`);
    }
  }
}

function packageNamePath(name) {
  return name.startsWith("@")
    ? path.join("node_modules", ...name.split("/"))
    : path.join("node_modules", name);
}

const outputDir = parseOutputDirectory(process.argv.slice(2));
const pluginDir = path.join(outputDir, "plugin");
const runtimeDependencies = packageJson.dependencies ?? {};

if (runtimeDependencies["@paperclipai/plugin-sdk"] !== "2026.916.1") {
  throw new Error("The pilot bundle must pin @paperclipai/plugin-sdk@2026.916.1");
}
assertExactRuntimeVersions(runtimeDependencies);

if (!existsSync(path.join(packageDir, "node_modules", ".bin", "tsc"))
  && !existsSync(path.join(repositoryRoot, "node_modules", ".bin", "tsc"))) {
  throw new Error("TypeScript is missing; install the Kubernetes plugin's development dependencies first");
}

if (existsSync(outputDir)) {
  throw new Error(`Output directory already exists; choose a fresh path: ${outputDir}`);
}

const tscBin = existsSync(path.join(packageDir, "node_modules", ".bin", "tsc"))
  ? path.join(packageDir, "node_modules", ".bin", "tsc")
  : path.join(repositoryRoot, "node_modules", ".bin", "tsc");

rmSync(path.join(packageDir, "dist"), { recursive: true, force: true });
execFileSync(tscBin, ["--project", path.join(packageDir, "tsconfig.json")], {
  cwd: packageDir,
  stdio: "inherit",
});

mkdirSync(pluginDir, { recursive: true });
for (const directory of ["dist", "manifests"]) {
  if (!existsSync(path.join(packageDir, directory))) {
    throw new Error(`Missing required build output: ${directory}`);
  }
  cpSync(path.join(packageDir, directory), path.join(pluginDir, directory), { recursive: true });
}
copyFileSync(path.join(packageDir, "README.md"), path.join(pluginDir, "README.md"));

const runtimePackageJson = {
  name: packageJson.name,
  version: packageJson.version,
  description: packageJson.description,
  license: packageJson.license,
  repository: packageJson.repository,
  type: "module",
  exports: packageJson.publishConfig.exports,
  main: packageJson.publishConfig.main,
  types: packageJson.publishConfig.types,
  paperclipPlugin: packageJson.paperclipPlugin,
  engines: packageJson.engines,
  dependencies: runtimeDependencies,
  devDependencies: packageJson.devDependencies,
};
writeFileSync(path.join(pluginDir, "package.json"), `${JSON.stringify(runtimePackageJson, null, 2)}\n`);
copyFileSync(path.join(packageDir, "package-lock.json"), path.join(pluginDir, "package-lock.json"));

execFileSync("npm", [
  "ci",
  "--prefix", pluginDir,
  "--omit=dev",
  "--ignore-scripts",
  "--no-audit",
  "--no-fund",
  "--fetch-retries=0",
  "--fetch-timeout=30000",
], {
  cwd: pluginDir,
  stdio: "inherit",
});

execFileSync("npm", ["ls", "--prefix", pluginDir, "--omit=dev", "--depth=0"], {
  cwd: pluginDir,
  stdio: "inherit",
});

for (const [name, version] of Object.entries(runtimeDependencies)) {
  const dependencyDir = path.join(pluginDir, packageNamePath(name));
  const dependencyJson = JSON.parse(readFileSync(path.join(dependencyDir, "package.json"), "utf8"));
  if (dependencyJson.version !== version) {
    throw new Error(`${name} resolved to ${dependencyJson.version}; expected exact ${version}`);
  }
  const resolvedDependencyDir = realpathSync(dependencyDir);
  if (!resolvedDependencyDir.startsWith(`${realpathSync(path.join(pluginDir, "node_modules"))}${path.sep}`)) {
    throw new Error(`${name} resolved outside the self-contained plugin node_modules tree`);
  }
}

const manifestUrl = pathToFileURL(path.join(pluginDir, "dist", "manifest.js")).href;
const pluginUrl = pathToFileURL(path.join(pluginDir, "dist", "plugin.js")).href;
const smokeImport = [
  "const [manifestUrl, pluginUrl, expectedVersion] = process.argv.slice(1);",
  "const [manifestModule, pluginModule] = await Promise.all([import(manifestUrl), import(pluginUrl)]);",
  "if (manifestModule.default?.version !== expectedVersion) throw new Error('manifest version mismatch');",
  "if (!pluginModule.default || typeof pluginModule.default !== 'object') throw new Error('plugin module did not load');",
].join("\n");
execFileSync(process.execPath, ["--input-type=module", "-e", smokeImport, manifestUrl, pluginUrl, packageJson.version], {
  cwd: pluginDir,
  stdio: "inherit",
});

console.log(`Built self-contained Paperclip plugin payload: ${pluginDir}`);
console.log(`Version: ${packageJson.version}; SDK: ${runtimeDependencies["@paperclipai/plugin-sdk"]}`);
