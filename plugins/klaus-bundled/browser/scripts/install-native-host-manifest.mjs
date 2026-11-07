#!/usr/bin/env node
import { pathToFileURL } from 'node:url';

export const migration = Object.freeze({
  version: 1,
  from: "scripts/install-native-host-manifest.mjs",
  command: 'klaus-browser',
  subcommand: "install-native-host-manifest",
  directExitCode: 64,
});

export default migration;

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.stderr.write("This browser utility moved to the Rust CLI: klaus-browser install-native-host-manifest.\n");
  process.exitCode = migration.directExitCode;
}
