//! std 锁的毒化恢复扩展。
//!
//! 本引擎以 panic + catch_unwind 作为正式的错误回收通道：业务错误通过 panic
//! 抛出，在 turn/工具边界统一回收为结构化错误后继续服务。这意味着 panic 是
//! 常规控制流的一部分，而 `std::sync::Mutex`/`RwLock` 在持锁线程 panic 时会把
//! 锁标记为"毒化"，之后所有 `lock().unwrap()` 都会连环 panic，形成级联崩溃。
//!
//! 在本引擎的语义下，毒化并不改变数据一致性预期——被回收的 panic 属于业务
//! 错误信道，受保护数据本身仍然有效，期望语义是恢复内部数据继续服务，而不是
//! 拒绝服务。本模块提供的扩展方法在锁被毒化时直接取回内部数据
//! （`PoisonError::into_inner`），使锁在一次被回收的 panic 之后仍可正常使用。

use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// 为 [`Mutex`] 提供毒化恢复的加锁方法。
pub trait MutexExt<T: ?Sized> {
    /// 加锁；若锁已被毒化，直接恢复内部数据继续服务。
    fn lock_recover(&self) -> MutexGuard<'_, T>;
}

impl<T: ?Sized> MutexExt<T> for Mutex<T> {
    fn lock_recover(&self) -> MutexGuard<'_, T> {
        self.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// 为 [`RwLock`] 提供毒化恢复的读写加锁方法。
pub trait RwLockExt<T: ?Sized> {
    /// 获取读锁；若锁已被毒化，直接恢复内部数据继续服务。
    fn read_recover(&self) -> RwLockReadGuard<'_, T>;

    /// 获取写锁；若锁已被毒化，直接恢复内部数据继续服务。
    fn write_recover(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T: ?Sized> RwLockExt<T> for RwLock<T> {
    fn read_recover(&self) -> RwLockReadGuard<'_, T> {
        self.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_recover(&self) -> RwLockWriteGuard<'_, T> {
        self.write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, RwLock};

    use super::{MutexExt, RwLockExt};

    /// 在持锁区间制造 panic 使锁毒化。
    fn poison_mutex(lock: &Mutex<i32>) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = lock.lock().unwrap();
            panic!("为测试制造的毒化");
        }));
        assert!(result.is_err());
        assert!(lock.is_poisoned());
    }

    fn poison_rwlock(lock: &RwLock<i32>) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = lock.write().unwrap();
            panic!("为测试制造的毒化");
        }));
        assert!(result.is_err());
        assert!(lock.is_poisoned());
    }

    #[test]
    fn mutex_lock_recover_returns_value_after_poison() {
        let lock = Mutex::new(7);
        poison_mutex(&lock);
        // 毒化后普通 lock() 返回 Err，而恢复语义方法取回数据继续服务。
        assert!(lock.lock().is_err());
        assert_eq!(*lock.lock_recover(), 7);
        *lock.lock_recover() = 8;
        assert_eq!(*lock.lock_recover(), 8);
    }

    #[test]
    fn mutex_lock_recover_works_without_poison() {
        let lock = Mutex::new(1);
        assert_eq!(*lock.lock_recover(), 1);
    }

    #[test]
    fn rwlock_read_recover_returns_value_after_poison() {
        let lock = RwLock::new(5);
        poison_rwlock(&lock);
        assert!(lock.read().is_err());
        assert_eq!(*lock.read_recover(), 5);
    }

    #[test]
    fn rwlock_write_recover_returns_value_after_poison() {
        let lock = RwLock::new(5);
        poison_rwlock(&lock);
        assert!(lock.write().is_err());
        *lock.write_recover() = 6;
        assert_eq!(*lock.read_recover(), 6);
    }

    #[test]
    fn rwlock_recover_works_without_poison() {
        let lock = RwLock::new(2);
        assert_eq!(*lock.read_recover(), 2);
        *lock.write_recover() = 3;
        assert_eq!(*lock.read_recover(), 3);
    }
}
