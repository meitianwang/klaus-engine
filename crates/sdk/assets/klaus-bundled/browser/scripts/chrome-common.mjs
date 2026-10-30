import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const CHROME_PREFERENCES_PATH_ENV = 'KLAUS_CHROME_PREFERENCES_PATH';
export const CHROME_USER_DATA_DIR_ENV = 'KLAUS_CHROME_USER_DATA_DIR';
export const CHROME_NATIVE_HOST_MANIFEST_PATH_ENV = 'KLAUS_CHROME_NATIVE_HOST_MANIFEST_PATH';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));

export function pluginRoot() {
  return path.resolve(scriptDir, '..');
}

export function loadExtensionConfig() {
  return JSON.parse(fs.readFileSync(path.join(scriptDir, 'extension-id.json'), 'utf8'));
}

export function resolveChromeUserDataDirectory() {
  if (process.env[CHROME_USER_DATA_DIR_ENV]) {
    return path.resolve(process.env[CHROME_USER_DATA_DIR_ENV]);
  }
  if (process.platform === 'darwin') {
    return path.join(os.homedir(), 'Library', 'Application Support', 'Google', 'Chrome');
  }
  if (process.platform === 'win32') {
    return path.join(process.env.LOCALAPPDATA || path.join(os.homedir(), 'AppData', 'Local'), 'Google', 'Chrome', 'User Data');
  }
  return path.join(os.homedir(), '.config', 'google-chrome');
}

export function resolveChromeProfileDirectory(userDataDirectory) {
  const localStateProfile = resolveChromeProfileDirectoryFromLocalState(userDataDirectory);
  if (localStateProfile) return localStateProfile;
  const latest = listChromeProfileDirectories(userDataDirectory).at(-1);
  if (latest) return latest;
  throw new Error(`Could not find a Chrome profile directory with Preferences in ${userDataDirectory}.`);
}

export function listChromeProfileDirectories(userDataDirectory) {
  if (!fs.existsSync(userDataDirectory)) {
    throw new Error(`Chrome user data directory does not exist: ${userDataDirectory}`);
  }
  return fs
    .readdirSync(userDataDirectory, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && (entry.name === 'Default' || /^Profile \d+$/.test(entry.name)))
    .map((entry) => entry.name)
    .filter((profile) => fs.existsSync(path.join(userDataDirectory, profile, 'Preferences')))
    .sort(compareChromeProfileDirectories);
}

export function resolveChromePreferencesPath() {
  if (process.env[CHROME_PREFERENCES_PATH_ENV]) {
    return path.resolve(process.env[CHROME_PREFERENCES_PATH_ENV]);
  }
  const userDataDirectory = resolveChromeUserDataDirectory();
  return path.join(userDataDirectory, resolveChromeProfileDirectory(userDataDirectory), 'Preferences');
}

export function readJsonIfPresent(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

export function nativeHostManifestPath(hostName) {
  if (process.env[CHROME_NATIVE_HOST_MANIFEST_PATH_ENV]) {
    return path.resolve(process.env[CHROME_NATIVE_HOST_MANIFEST_PATH_ENV]);
  }
  if (process.platform === 'darwin') {
    return path.join(os.homedir(), 'Library', 'Application Support', 'Google', 'Chrome', 'NativeMessagingHosts', `${hostName}.json`);
  }
  if (process.platform === 'win32') {
    return path.join(process.env.LOCALAPPDATA || path.join(os.homedir(), 'AppData', 'Local'), 'Klaus', 'ChromeNativeHosts', `${hostName}.json`);
  }
  return path.join(os.homedir(), '.config', 'google-chrome', 'NativeMessagingHosts', `${hostName}.json`);
}

function resolveChromeProfileDirectoryFromLocalState(userDataDirectory) {
  let localState = null;
  try {
    localState = readJsonIfPresent(path.join(userDataDirectory, 'Local State'));
  } catch {
    return null;
  }
  const profile = localState?.profile;
  if (!profile || typeof profile !== 'object') return null;
  if (isUsableProfile(userDataDirectory, profile.last_used)) return profile.last_used;
  if (Array.isArray(profile.last_active_profiles)) {
    return profile.last_active_profiles
      .filter((candidate) => isUsableProfile(userDataDirectory, candidate))
      .sort(compareChromeProfileDirectories)
      .at(-1) || null;
  }
  return null;
}

function isUsableProfile(userDataDirectory, profileDirectory) {
  return typeof profileDirectory === 'string' &&
    fs.existsSync(path.join(userDataDirectory, profileDirectory, 'Preferences'));
}

function compareChromeProfileDirectories(a, b) {
  return profileSortKey(a) - profileSortKey(b);
}

function profileSortKey(profileDirectory) {
  if (profileDirectory === 'Default') return 0;
  const match = profileDirectory.match(/^Profile (\d+)$/);
  return match ? Number.parseInt(match[1], 10) : -1;
}
