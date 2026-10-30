/* SPDX-License-Identifier: MIT
 * Capability syscall adapter for the embedded namespace launcher.
 * No dynamically linked capability library is required.
 */
#ifndef KLAUS_CAPABILITY_H
#define KLAUS_CAPABILITY_H
#include <linux/capability.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <errno.h>
#include <strings.h>
typedef int cap_value_t;
static inline int capget(struct __user_cap_header_struct *header,
                         struct __user_cap_data_struct *data) {
  return (int) syscall(SYS_capget, header, data);
}
static inline int capset(struct __user_cap_header_struct *header,
                         const struct __user_cap_data_struct *data) {
  return (int) syscall(SYS_capset, header, data);
}
static inline int cap_from_name(const char *name, cap_value_t *value) {
  static const char *names[] = {
    "chown", "dac_override", "dac_read_search", "fowner", "fsetid", "kill",
    "setgid", "setuid", "setpcap", "linux_immutable", "net_bind_service",
    "net_broadcast", "net_admin", "net_raw", "ipc_lock", "ipc_owner",
    "sys_module", "sys_rawio", "sys_chroot", "sys_ptrace", "sys_pacct",
    "sys_admin", "sys_boot", "sys_nice", "sys_resource", "sys_time",
    "sys_tty_config", "mknod", "lease", "audit_write", "audit_control",
    "setfcap", "mac_override", "mac_admin", "syslog", "wake_alarm",
    "block_suspend", "audit_read", "perfmon", "bpf", "checkpoint_restore"
  };
  if (strncasecmp(name, "cap_", 4) == 0) name += 4;
  for (unsigned int index = 0; index < sizeof(names) / sizeof(names[0]); index++) {
    if (strcasecmp(name, names[index]) == 0) {
      *value = (cap_value_t) index;
      return 0;
    }
  }
  errno = EINVAL;
  return -1;
}
#endif
