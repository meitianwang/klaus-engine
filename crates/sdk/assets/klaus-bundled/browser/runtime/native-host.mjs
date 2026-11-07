#!/usr/bin/env node
import { pathToFileURL } from 'node:url';

export const migration = Object.freeze({
  version: 1,
  from: "runtime/native-host.mjs",
  command: 'klaus-browser',
  subcommand: "install-native-host-manifest",
  directExitCode: 64,
});

export default migration;

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.stderr.write("Legacy JavaScript native host disabled; run klaus-browser install-native-host-manifest.\n");
  process.exitCode = migration.directExitCode;
}
